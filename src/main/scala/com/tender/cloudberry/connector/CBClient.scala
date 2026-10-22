package com.tender.cloudberry.connector

import com.tender.cloudberry.connector.CBClient.makeConnProps
import org.apache.spark.internal.Logging
import org.apache.spark.sql.tender.cloudberry.connector.CBOptionsFactory

import java.sql.{Connection, DriverManager, SQLWarning}
import java.util.Properties
import scala.language.{postfixOps, reflectiveCalls}
import scala.util.Try
import scala.collection.mutable.{HashMap => MutableHashMap, Map => MutableMap}

/**
 * tender: connections come from a pool shared in the JVM (the driver) by all operations with the same
 * connection settings — see [[CBClient.pool]]. Before, every read/write (CloudberryRowSet) created its own
 * pool and never closed it: its idle sessions stayed open in Cloudberry until GC (2 per operation).
 */
class CBClient(optionsFactory: CBOptionsFactory) {

  def getConnection(): Connection = CBClient.pool(optionsFactory).getConnection

}

case object CBClient extends Logging {

  import java.net.{InetAddress, InetSocketAddress, ServerSocket}
  import org.apache.commons.dbcp2.BasicDataSource

  /** tender: connection settings (incl. pool options) -> pool, closed at JVM shutdown */
  private val pools = new java.util.concurrent.ConcurrentHashMap[String, BasicDataSource]()
  sys.addShutdownHook {
    import scala.collection.JavaConverters._
    pools.values().asScala.foreach(p => Try(p.close()))
  }

  /**
   * tender: the shared pool for these connection settings: at most pool.maxSize sessions, idle ones are
   * closed after pool.idleMs (keeping pool.minIdle), waiting for a free one — pool.timeoutMs
   */
  private[connector] def pool(optionsFactory: CBOptionsFactory): BasicDataSource = {
    import scala.collection.JavaConverters._
    val props = makeConnProps(optionsFactory).asScala.toSeq.sorted.map { case (key, value) => s"$key=$value" }
      .mkString(";")
    val key = Seq(optionsFactory.url, optionsFactory.user, optionsFactory.password, props,
      optionsFactory.poolMaxSize, optionsFactory.poolMinIdle, optionsFactory.poolTimeoutMs, optionsFactory.poolIdleMs)
      .mkString("\u0000")
    pools.computeIfAbsent(key, _ => {
      val pool = new BasicDataSource()
      pool.setUrl(optionsFactory.url)
      if (optionsFactory.user != null)
        pool.setUsername(optionsFactory.user)
      if (optionsFactory.password != null)
        pool.setPassword(optionsFactory.password)
      pool.setMaxWaitMillis(optionsFactory.poolTimeoutMs)
      pool.setMaxTotal(optionsFactory.poolMaxSize)
      pool.setMaxIdle(optionsFactory.poolMaxSize)
      pool.setMinIdle(optionsFactory.poolMinIdle)
      pool.setMinEvictableIdleTimeMillis(optionsFactory.poolIdleMs)
      pool.setTimeBetweenEvictionRunsMillis(math.max(1000L, optionsFactory.poolIdleMs / 2))
      pool.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
      pool.setAutoCommitOnReturn(false)
      pool.setDefaultAutoCommit(false)
      pool.setRollbackOnReturn(true)
      pool.setConnectionProperties(props)
      logInfo(s"New JDBC pool for ${optionsFactory.url}: maxSize=${optionsFactory.poolMaxSize}, " +
        s"minIdle=${optionsFactory.poolMinIdle}, idleMs=${optionsFactory.poolIdleMs}")
      pool
    })
  }

  private def makeConnProps(optionsFactory: CBOptionsFactory): Properties = {
    val props = optionsFactory.getJDBCOptions().asProperties
    props.remove("url")
    if ((optionsFactory.dbSchema != null) && optionsFactory.dbSchema.nonEmpty) {
      var searchPathApply: String = optionsFactory.dbSchema
      val schemas = searchPathApply.split("[, ]+]")
      if (!schemas.contains("public"))
        searchPathApply += ",public"
      props.setProperty("currentSchema", searchPathApply)
    }
    props
  }

  def getConn(optionsFactory: CBOptionsFactory) : Connection = {
    val conn = DriverManager.getConnection(optionsFactory.url, makeConnProps(optionsFactory))
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    conn
  }

  def checkGpfdistIsUp(localIp: String, port: Int): Boolean = using(new ServerSocket) {
    serverSocket =>
      try {
        //serverSocket.setReuseAddress(false) // this required only on OSX
        serverSocket.bind(new InetSocketAddress(InetAddress.getByName(localIp), port), 1)
        false
      } catch {
        case _: Exception =>
          true
      }
  }

  private def logDbWarnings(pWarning: SQLWarning, msgLogLevel: String): Unit = {
    var warning: SQLWarning = pWarning
    while (warning != null)
    {
      val msg = warning.getMessage
      val state = warning.getSQLState
      msgLogLevel match {
        case "DEBUG" => logDebug(s"${state}: ${msg}")
        case "INFO" => logInfo(s"${state}: ${msg}")
        case "WARN" => logWarning(s"${state}: ${msg}")
        case _ =>
      }
      warning = warning.getNextWarning
    }
  }

  /** tender: onStatement gets the statement before it runs — e.g. to cancel it from another thread */
  def executeStatement(conn: Connection, sql: String, msgLogLevel: String = "OFF",
                       onStatement: java.sql.Statement => Unit = _ => ()): Int = {
    using(conn.createStatement()) {
      statement => {
        onStatement(statement)
        val ret = statement.executeUpdate(sql)
        if (msgLogLevel != "OFF") logDbWarnings(statement.getWarnings, msgLogLevel)
        ret
      }
    }
  }

  def using[A, B <: {
    def close(): Unit
  }](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }

  private def createDbSchemeIfNotExists(conn: Connection, schemaNameParam: String): Boolean = {
    var schemaExists: Boolean = false
    val schemaName: String = if (!schemaNameParam.contains("\"")) schemaNameParam.toLowerCase() else schemaNameParam
    CBClient.using(conn.prepareStatement("select count(1) cnt " +
      "from information_schema.schemata where schema_name = ?")) {
      st => {
        st.setString(1, schemaName.replaceAll("\"", ""))
        using(st.executeQuery()) {
          rs => {
            while (rs.next()) {
              schemaExists = rs.getInt(1)==1
            }
          }
        }
      }
    }
    if (!schemaExists) {
      logInfo(s"Will create DB schema ${schemaName} because it doesn't exists..")
      CBClient.using(conn.createStatement()) {
        st => st.executeUpdate(s"create schema ${schemaName}")
      }
      if (!conn.getAutoCommit)
        conn.commit()
    } else {
      logInfo(s"DB schema ${schemaName} exists.")
    }
    !schemaExists
  }

  private def getDbObjSearchPath(conn: Connection): String = {
    var ret: String = ""
    CBClient.using(conn.createStatement()) {
      st => {
        using(st.executeQuery(s"show search_path")) {
          rs => if (rs.next()) ret = rs.getString(1).replaceAll("\\s+", "")
        }
      }
    }
    ret
  }

  /**
   *Validates & activates passed in database objects search path, which can contain single database schema name,
   * or comma separated list of database schema names. Creates primary (first in the list) database schema
   * if it doesn't exists. Returns the primary database schema name, or null if an input searchPath is null or empty.
   * @param conn Cloudberry database JDBC connection
   * @param searchPath Value passed in via dbschema connector option. Can be null.
   * @return name of the database schema where destination table should be created (for write operations)
   */
  def checkDbObjSearchPath(conn: Connection, searchPath: String): String = {
    var ret: String = null
    if ((searchPath != null) && searchPath.nonEmpty) {
      var searchPathApply: String = searchPath.replaceAll("\\s+", "")
      val schemas = searchPath.split("[,]+]")
      if (schemas.nonEmpty) {
        ret = schemas(0)
        if (!schemas.contains("public"))
          searchPathApply += ",public"
        createDbSchemeIfNotExists(conn, schemas(0))
        val currSearchPath = getDbObjSearchPath(conn)
        if (currSearchPath != searchPathApply) {
          CBClient.using(conn.createStatement()) {
            st => st.executeUpdate(s"set search_path to ${searchPathApply}")
          }
        }
      }
    }
    logInfo(s"Current Cloudberry name space search path: ${getDbObjSearchPath(conn)}")
    ret
  }

  /**
   * Returns a map of node server name -> set of Cloudberry primary segment IDs it holds.
   * <p>Excludes master node.
   * <p>Also only segments in the UP status returned.
   * @param conn Cloudberry connection
   * @return
   */
  def nodeNamesWithSegments(conn: Connection): Map[String, Set[String]] = {
    val ret: MutableMap[String, Set[String]] = MutableMap[String, Set[String]]()
    val select = """select  hostname, array_agg(content::text) segment_ids
                   |from    gp_segment_configuration
                   |where   content >= 0
                   |        and role = 'p'
                   |        and status = 'u'
                   |        /*and mode = 's'*/
                   |group by hostname""".stripMargin
    using(conn.prepareStatement(select)) {
      statement => {
       using(statement.executeQuery()) {
         rs => {
           while (rs.next()) {
             ret.put(rs.getString(1), rs.getArray(2).getArray().asInstanceOf[Array[String]].toSet)
           }
         }
       }
      }
    }
    ret.toMap
  }

  def tableExists(conn: Connection, table: String): Boolean = {
    val query = s"SELECT * FROM $table WHERE 1=0"
    Try {
      // tender: a plain statement (one-shot): a PreparedStatement of the same text becomes a server-side one after
      // 5 runs (prepareThreshold) and fails once the table is created again with other column types ("cached plan
      // must not change result type") — the table was taken for absent, a write failed on CREATE ("already exists")
      val statement = conn.createStatement()
      try {
        statement.executeQuery(query)
      } finally {
        statement.close()
        if (!conn.getAutoCommit) {
          conn.clearWarnings()
          conn.rollback()
        }
      }
    }.isSuccess
  }

  /**
   * tender: the Cloudberry version of the server — "2.1.0" of "PostgreSQL 14.4 (Apache Cloudberry 2.1.0-incubating
   * build ...)", "1.6.0" of "(Cloudberry Database 1.6.0 build ...)"; None — not Cloudberry (Greenplum, PostgreSQL)
   */
  def queryCBVersion(conn: Connection): Option[String] = {
    using(conn.createStatement()) {
      st => using(st.executeQuery("select substring(version() from '(?:Apache Cloudberry|Cloudberry Database) ([0-9.]+)')")) {
        rs => if (rs.next()) Option(rs.getString(1)) else None
      }
    }
  }

  /** tender: the URLs whose server version is logged (once per JVM) */
  private val checkedServers = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()

  /**
   * tender: logs the Cloudberry version of the server at the first operation on its URL; not Cloudberry — a
   * warning (Greenplum 7 has the same protocol, but the connector is checked on Cloudberry only)
   */
  def checkServer(conn: Connection, url: String): Unit =
    if (checkedServers.add(url)) {
      Try(queryCBVersion(conn)).recover { case e: Exception => logDebug(s"version() of $url: $e"); None }.get match {
        case Some(version) => logInfo(s"Cloudberry $version at $url")
        case None => logWarning(s"Not a Cloudberry server at $url (version() has no 'Apache Cloudberry'): the connector " +
          "is checked on Cloudberry 2.x only")
      }
      if (!conn.getAutoCommit)
        conn.rollback()
    }

  /**
   * tender: the primary segments by the catalog of the coordinator (gp_segment_configuration). Before:
   * gp_toolkit.gp_workfile_usage_per_segment — a query dispatched to the segments, and gp_toolkit is an extension
   * in Cloudberry: a database created from template0 has none
   */
  def queryNSegments(conn: Connection): Int = {
    using(conn.createStatement()) {
      statement => using(statement.executeQuery("select count(1) from gp_segment_configuration where content >= 0 and role = 'p'")) {
        rs => if (rs.next()) rs.getInt(1) else 0
      }
    }
  }

  /**
   * tender: catalog statistics of a table with all its partitions (pg_inherits, any depth): rows and bytes
   * (relpages x block_size) as the last ANALYZE / VACUUM left them on the coordinator — no dispatch to the
   * segments. None — no such relation, never analyzed (relpages 0), or the lookup failed. Only the relations
   * with storage: ANALYZE of a partitioned table (relkind p) also puts the rows of all its partitions into its
   * reltuples, and its relpages is -1 (PostgreSQL 14) — they would count twice.
   * @param table table name as in SQL (unqualified — by the search_path, quoted parts kept)
   * @return (rows, 0 when not known; bytes)
   */
  def queryTableStats(conn: Connection, table: String): Option[(Long, Long)] = Try {
    val oids = partitionTree(conn, table).map(_._1)
    using(conn.createStatement()) {
      st => using(st.executeQuery("select coalesce(sum(greatest(reltuples, 0)::float8), 0)::bigint, " +
        "(coalesce(sum(greatest(relpages, 0)::bigint), 0) * current_setting('block_size')::bigint)::bigint " +
        s"from pg_class where oid in (${oids.mkString(", ")}) and relkind <> 'p'")) {
        rs => {
          rs.next()
          val (rows, bytes) = (rs.getLong(1), rs.getLong(2))
          if (bytes > 0) Some((rows, bytes)) else None
        }
      }
    }
  }.recover {
    case e: Exception =>
      logDebug(s"No statistics of $table: $e")
      if (!conn.getAutoCommit)
        conn.rollback()
      None
  }.get

  /**
   * tender: the leaf partitions of a partitioned table (any depth: pg_inherits keeps them for the PostgreSQL
   * syntax and the classic Greenplum one alike) for a split read: qualified name, bytes by the catalog
   * (relpages x block_size, 0 — never analyzed or empty), whether the session user may SELECT from it
   * directly. Empty — not a partitioned table, no such relation, or the lookup failed.
   */
  def queryLeafPartitions(conn: Connection, table: String): Seq[(String, Long, Boolean)] = Try {
    val leaves = partitionTree(conn, table).filter(!_._2).map(_._1)
    if (leaves.size < 2) Seq.empty
    else using(conn.createStatement()) {
      st => using(st.executeQuery("select quote_ident(n.nspname) || '.' || quote_ident(c.relname), " +
        "(c.relpages::bigint * current_setting('block_size')::bigint)::bigint, has_table_privilege(c.oid, 'SELECT') " +
        "from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
        s"where c.oid in (${leaves.mkString(", ")}) order by 1")) {
        rs => Iterator.continually(rs).takeWhile(_.next()).map(rs => (rs.getString(1), rs.getLong(2), rs.getBoolean(3))).toList
      }
    }
  }.recover {
    case e: Exception =>
      logWarning(s"No leaf partitions of $table: $e")
      if (!conn.getAutoCommit)
        conn.rollback()
      Seq.empty
  }.get

  /**
   * tender: how many segments Cloudberry dispatches a query to — the first "segments: N" of its EXPLAIN (the top
   * Gather Motion; 1 — dispatched directly to one segment); None — not found or EXPLAIN failed
   */
  def dispatchedSegments(conn: Connection, sql: String): Option[Int] = Try {
    using(conn.createStatement()) {
      st => using(st.executeQuery(s"explain $sql")) {
        rs => {
          val plan = Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).mkString("\n")
          """segments: (\d+)""".r.findFirstMatchIn(plan).map(_.group(1).toInt)
        }
      }
    }
  }.recover {
    case e: Exception =>
      logWarning(s"EXPLAIN $sql: $e")
      if (!conn.getAutoCommit)
        conn.rollback()
      None
  }.get

  /**
   * tender: a table and all its partitions (pg_inherits, level by level): oid -> whether it has partitions
   * @param table table name as in SQL (unqualified — by the search_path, quoted parts kept)
   */
  private def partitionTree(conn: Connection, table: String): Seq[(Long, Boolean)] = {
    val root: Long = using(conn.prepareStatement("select ?::regclass::oid")) {
      st => {
        st.setString(1, table)
        using(st.executeQuery()) {
          rs => {
            rs.next()
            rs.getLong(1)
          }
        }
      }
    }
    var tree = Seq.empty[(Long, Boolean)]
    var level = Seq(root)
    while (level.nonEmpty) {
      val edges = using(conn.createStatement()) {
        st => using(st.executeQuery(s"select inhparent, inhrelid from pg_inherits where inhparent in (${level.mkString(", ")})")) {
          rs => Iterator.continually(rs).takeWhile(_.next()).map(rs => (rs.getLong(1), rs.getLong(2))).toList
        }
      }
      val parents = edges.map(_._1).toSet
      tree ++= level.map(oid => (oid, parents.contains(oid)))
      level = edges.map(_._2)
    }
    tree
  }

  /**
   * Extracts distribution policy clause from the DML of given Cloudberry DB table.
   * @param conn DB connection
   * @param tblSchema schema (namespace) of the table
   * @param tblName table name without namespace
   * @param tblKind table storage kind: h=heap, a=append-optimized, c=column-oriented, v=virtual, x=external table
   * @return distribution clause of DML
   */
  def getTableDistributionPolicy(conn: Connection, tblSchema: String, tblName: String, tblKind: String = "h"): String = {
    var ret: String = ""
    val schemas = if (tblSchema == null || tblSchema.isEmpty)
        """"$user",public""".split(",")
        else tblSchema.split(",")
    val sqlArr = conn.createArrayOf("varchar", schemas.toArray)
    // tender: Cloudberry has no pg_class.relstorage (Greenplum 5/6; the storage is the access method, relam):
    // the table kind is relkind — r/p (heap, append-optimized, PAX tables), f (external = foreign tables)
    val query: String =
      s"""select pg_get_table_distributedby(c.oid) distributed_by
         |from   pg_class c
         |       join pg_namespace n on (c.relnamespace = n.oid)
         |where  n.nspname = any (?)
         |       and c.relname = ?
         |       and c.relkind::text = any (?)
         |order by n.nspname, c.relname limit 1""".stripMargin
    using(conn.prepareStatement(query)) {
      statement => {
        statement.setArray(1, sqlArr)
        statement.setString(2, tblName)
        statement.setArray(3, conn.createArrayOf("varchar", tblKind match {
          case "x" => Array[AnyRef]("f")
          case "v" => Array[AnyRef]("v")
          case _ => Array[AnyRef]("r", "p")
        }))
        using(statement.executeQuery()) {
          rs => {
            while (rs.next())
              ret = rs.getString(1)
          }
        }
      }
    }
    ret
  }

  /**
   * Recognize & normalize table DML distribution clause.
   * Also outputs a column list suitable for primary key creation, if applicable.
   * <p> An empty strings returned in the case of invalid input.
   * <p>Allowed input examples:
   * <p>replicated
   * <p>randomly
   * <p>distributed  randomly
   * <p>distributed by (col1, col2)
   * <p>col1, col2
   * @param src
   * @return tuple: (normalized distribution clause, cleaned col name list or empty string in the case
   *         of <b>replicated</b> or <b>randomly</b> policy)
   */
  def formatDistributedByClause(src: String): (String, String) = {
    if (src == null || src.isEmpty)
      return ("", "")
    val replicatedPattern = """\A\s*(?i)(distributed\s+)?(replicated)\s*\z""".r
    val randomlyPattern = """\A\s*(?i)(distributed\s+)?(randomly)\s*\z""".r
    val byColListPattern = """\A\s*(?i)(distributed\s+by\s*\()?([^)]+)(\))?\s*\z""".r
    val colPattern = """\A(?i)\s*(\w+)(\s.*)?\z""".r
    val quotedColPattern = """\A(?i)\s*(["][^"]+["])(\s.*)?\z""".r
    src match {
      case replicatedPattern(_, arg) => ("distributed replicated", "")
      case randomlyPattern(_, arg) => ("distributed randomly", "")
      case byColListPattern(_, arg, _) => {
        val colNames = arg.trim.split(",").map {
          case colPattern(colName, _) => colName
          case quotedColPattern(colName, _) => colName
          case _ => arg.trim
        }.mkString(",")
        (s"distributed by ($arg)", colNames)
      }
      case _ => ("", "")
    }
  }
}
