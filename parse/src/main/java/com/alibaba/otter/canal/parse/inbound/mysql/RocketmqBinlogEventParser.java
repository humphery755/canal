package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;

import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.rocketmq.RocketMQEventStore;

/**
 * 基于RocketMQ解析
 * 
 * @author humphery 2017-6-15 下午04:07:33
 * @version 1.0.0
 */
public class RocketmqBinlogEventParser extends AbstractMysqlEventParser implements CanalEventParser {

	// 数据库信息
	private AuthenticationInfo masterInfo;
	private EntryPosition masterPosition; // binlog信息
	private MysqlConnection metaConnection; // 查询meta信息的链接
	private TableMetaCache tableMetaCache; // 对应meta
	private RocketMQEventStore eventStore;

	private boolean needWait = false;
	private int bufferSize = 16 * 1024;
	private Long lastPosition;
	private Long prePosition;
	private Long pipelineId;

	public RocketmqBinlogEventParser(Long pipelineId) {
		// this.runningInfo = new AuthenticationInfo();
		this.pipelineId = pipelineId;
	}

	@Override
	protected ErosaConnection buildErosaConnection() {
		return buildMysqlConnection();
	}

	@Override
	protected void preDump(ErosaConnection connection) {
		metaConnection = buildMysqlConnection();
		try {
			metaConnection.connect();
		} catch (IOException e) {
			throw new CanalParseException(e);
		}

		tableMetaCache = new TableMetaCache(metaConnection);
		((LogEventConvert) binlogParser).setTableMetaCache(tableMetaCache);
	}

	@Override
	protected void afterDump(ErosaConnection connection) {
		if (metaConnection != null) {
			try {
				metaConnection.disconnect();
			} catch (IOException e) {
				logger.error("ERROR # disconnect meta connection for address:{}",
						metaConnection.getConnector().getAddress(), e);
			}
		}
	}

	public void start() throws CanalParseException {
		if (runningInfo == null) { // 第一次链接主库
			runningInfo = masterInfo;
		}
		super.start();
	}

	@Override
	public void stop() {
		if (metaConnection != null) {
			try {
				metaConnection.disconnect();
			} catch (IOException e) {
				logger.error("ERROR # disconnect meta connection for address:{}",
						metaConnection.getConnector().getAddress(), e);
			}
		}

		if (tableMetaCache != null) {
			tableMetaCache.clearTableMeta();
		}

		super.stop();
	}

	private MysqlConnection buildMysqlConnection() {
		MysqlConnection connection = new MysqlConnection(runningInfo.getAddress(), runningInfo.getUsername(),
				runningInfo.getPassword(), connectionCharsetNumber, runningInfo.getDefaultDatabaseName()) {

			public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
				try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                }
			}

			public void disconnect() throws IOException {
				super.disconnect();
			}

			public MysqlConnection fork() {
				return this;
			}
		};
		connection.getConnector().setReceiveBufferSize(64 * 1024);
		connection.getConnector().setSendBufferSize(64 * 1024);
		connection.getConnector().setSoTimeout(30 * 1000);
		connection.setCharset(connectionCharset);
		return connection;
	}

	protected CanalEntry.Entry parseAndProfilingIfNecessary(Object bod) throws Exception {

		if (parsedEventCount.incrementAndGet() < 0) {
			parsedEventCount.set(0);
		}
		return (CanalEntry.Entry) bod;
	}

	@Override
	protected EntryPosition findStartPosition(ErosaConnection connection) {
		// 处理逻辑
		// 1. 首先查询上一次解析成功的最后一条记录
		// 2. 存在最后一条记录，判断一下当前记录是否发生过主备切换
		// // a. 无机器切换，直接返回
		// // b. 存在机器切换，按最后一条记录的stamptime进行查找
		// 3. 不存在最后一条记录，则从默认的位置开始启动
		LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
		if (logPosition == null) {// 找不到历史成功记录
			EntryPosition entryPosition = masterPosition;

			// 判断一下是否需要按时间订阅
			if (StringUtils.isEmpty(entryPosition.getJournalName())) {
				// 如果没有指定binlogName，尝试按照timestamp进行查找
				if (entryPosition.getTimestamp() != null) {
					return new EntryPosition(entryPosition.getTimestamp());
				}
			} else {
				if (entryPosition.getPosition() != null) {
					// 如果指定binlogName + offest，直接返回
					return entryPosition;
				} else {
					return new EntryPosition(entryPosition.getTimestamp());
				}
			}
		} else {
			return logPosition.getPostion();
		}

		return null;
	}

	public void ack(Position position) {
	}
	
	public void rollback() {
		lastPosition = prePosition;
	}

	// ========================= setter / getter =========================

	public void setLogPositionManager(CanalLogPositionManager logPositionManager) {
		this.logPositionManager = logPositionManager;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public void setMasterPosition(EntryPosition masterPosition) {
		this.masterPosition = masterPosition;
	}

	public void setMasterInfo(AuthenticationInfo masterInfo) {
		this.masterInfo = masterInfo;
	}

	public boolean isNeedWait() {
		return needWait;
	}

	public void setNeedWait(boolean needWait) {
		this.needWait = needWait;
	}

	public void setEventStore(CanalEventStore eventStore2) {
		this.eventStore = (RocketMQEventStore)eventStore2;
	}

}
