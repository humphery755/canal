package com.alibaba.otter.canal.store.rocketmq;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.CanalStoreScavenge;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 基于内存buffer构建内存memory store
 * 
 * <pre>
 * 变更记录：
 * 1. 新增BatchMode类型，支持按内存大小获取批次数据，内存大小更加可控.
 *   a. put操作，会首先根据bufferSize进行控制，然后再进行bufferSize * bufferMemUnit进行控制. 因存储的内容是以Event，如果纯依赖于memsize进行控制，会导致RingBuffer出现动态伸缩
 * </pre>
 * 
 * @author jianghang 2012-6-20 上午09:46:31
 * @version 1.0.0
 */
public class RocketMQEventStore extends AbstractCanalStoreScavenge
		implements CanalEventStore<Event>, CanalStoreScavenge {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final long INIT_SQEUENCE = -1;
	private AtomicLong getSequence = new AtomicLong(INIT_SQEUENCE); // 代表当前get操作读取的最后一条的位置
	private AtomicLong ackSequence = new AtomicLong(INIT_SQEUENCE); // 代表当前ack操作的最后一条的位置
	private int sendMsgTimeout = 99000;
	private final static Map<String,InetSocketAddress> INET_SOCKETADDR = new HashMap();

	// 阻塞put/get操作控制信号
	private ReentrantLock lock = new ReentrantLock();

	private boolean producer = false;
	private boolean consumer = false;
	private final DefaultMQProducer mqproducer;
	private final DefaultMQPullConsumer mqconsumer;
	private MessageQueue mqMsgqueue;

	private String nameSvrAddresses;
	private String topic;
	private Long pipelineId = 0l;
//	private Long lastCommitOffset;
	private boolean ddlIsolation = false;

	public RocketMQEventStore() {
		mqproducer = new DefaultMQProducer();
		mqconsumer = new DefaultMQPullConsumer();
	}

	public void start() throws CanalStoreException {
		logger.info("start RocketMQEventStore for {}-{} with parameters:{}", pipelineId, destination,
				new Object[] { nameSvrAddresses, topic });
		super.start();
		if (producer) {
			mqproducer.setProducerGroup("canalstore_pg-" + destination);
			mqproducer.setNamesrvAddr(nameSvrAddresses);
			try {
				mqproducer.start();
			} catch (MQClientException e) {
				throw new CanalStoreException("rocketmq-producer don't be started.", e);
			}
		}
		if (consumer) {
			mqconsumer.setConsumerGroup("canalstore_cg-" + destination + "-" + pipelineId);
			mqconsumer.setNamesrvAddr(nameSvrAddresses);
			try {
				mqconsumer.start();
				Set<MessageQueue> mqs;
				try {
					mqs = mqconsumer.fetchSubscribeMessageQueues(topic);
				} catch (MQClientException e) {
					throw new CanalStoreException("rocketmq-consumer fetch faliure.", e);
				}

				for (MessageQueue _mq : mqs) {
					if (_mq.getQueueId() == 0) {
						mqMsgqueue = _mq;
						break;
					}
				}
			} catch (MQClientException e) {
				throw new CanalStoreException("rocketmq-consumer don't be started.", e);
			}
		}
	}

	public void stop() throws CanalStoreException {
		super.stop();

		cleanAll();
		if (mqproducer != null)
			mqproducer.shutdown();
		if (mqconsumer != null)
			mqconsumer.shutdown();
	}

	public void put(List<Event> data) throws InterruptedException, CanalStoreException {
		if (data == null || data.isEmpty()) {
			return;
		}

		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {

			doPut(data, sendMsgTimeout);

		} finally {
			lock.unlock();
		}
	}

	public boolean put(List<Event> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
		if (data == null || data.isEmpty()) {
			return true;
		}

		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			doPut(data, timeout);
			return true;
		} finally {
			lock.unlock();
		}
	}

	public boolean tryPut(List<Event> data) throws CanalStoreException {
		if (data == null || data.isEmpty()) {
			return true;
		}

		final ReentrantLock lock = this.lock;
		lock.lock();
		try {

			doPut(data, sendMsgTimeout);
			return true;
		} finally {
			lock.unlock();
		}
	}

	public void put(Event data) throws InterruptedException, CanalStoreException {
		put(Arrays.asList(data));
	}

	public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
		return put(Arrays.asList(data), timeout, unit);
	}

	public boolean tryPut(Event data) throws CanalStoreException {
		return tryPut(Arrays.asList(data));
	}

	/**
	 * 执行具体的put操作
	 */
	private void doPut(List<Event> data, long timeout) {
		String tag, key;
		for (Event event : data) {
			tag = event.getLogIdentity().getSourceAddress() + "|" + event.getLogIdentity().getSlaveId() + "|"
					+ event.getEntry().getHeader().getSchemaName() + "|" + event.getEntry().getHeader().getTableName();

			key = event.getEntry().getHeader().getLogfileName() + "-" + event.getEntry().getHeader().getLogfileOffset();

			Message msg = new Message(topic, tag, key, event.getEntry().toByteArray());
			try {
				SendResult sendResult = mqproducer.send(msg, new MessageQueueSelector() {
					public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
						return mqs.get(0);
					}
				}, tag, timeout);
				if (sendResult == null) {
					throw new CanalStoreException("mqproducer.send return null.");
				}
			} catch (Exception e) {
				throw new CanalStoreException("mqproducer.send failure.", e);
			}
		}
	}

	public Events<Event> get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			return doGet(start, batchSize, -1);
		} finally {
			lock.unlock();
		}
	}

	public Events<Event> get(Position start, int batchSize, long timeout, TimeUnit unit)
			throws InterruptedException, CanalStoreException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			return doGet(start, batchSize, timeout);
		} finally {
			lock.unlock();
		}
	}

	public Events<Event> tryGet(Position start, int batchSize) throws CanalStoreException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return doGet(start, batchSize, -1);
		} finally {
			lock.unlock();
		}
	}

	private LogPosition createPosition(Long offset) {
		EntryPosition position = new EntryPosition();
		position.setJournalName(topic);
		position.setPosition(offset);
		position.setTimestamp(0l);

		LogPosition logPosition = new LogPosition();
		logPosition.setPostion(position);
		logPosition.setIdentity(new LogIdentity());
		return logPosition;
	}

	private Events<Event> doGet(Position start, int batchSize, long timeout) throws CanalStoreException {
		LogPosition startPosition = (LogPosition) start;
		long current = getSequence.get();
		long next = current;
		long end = current;

		PullResult pullResult = null;
		
		if (startPosition == null || !startPosition.getPostion().isIncluded()) { // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
            next = next + 1;
        }
		// if(offset>0 && offset=las)
		try {
			if (timeout == -1)
				pullResult = mqconsumer.pullBlockIfNotFound(mqMsgqueue, null, next, batchSize);
			else
				pullResult = mqconsumer.pull(mqMsgqueue, null, next, batchSize, timeout);
		} catch (Exception e) {
			throw new CanalStoreException("mqconsumer.pullBlockIfNotFound failure.", e);
		}
		if (pullResult == null)
			throw new CanalStoreException("mqconsumer.pullBlockIfNotFound return null.");

		List<MessageExt> msgs = pullResult.getMsgFoundList();

		if (msgs == null || CollectionUtils.isEmpty(msgs)) {
			logger.debug("getWithoutAck successfully, topic:{} lastPosition:{} batchSize:{} but result is null",
					new Object[] { topic, next, batchSize });
			return new Events<Event>();
		}

		end = next + msgs.size() - 1;

		Events<Event> result = new Events<Event>();
		List<Event> entrys = result.getEvents();
		// 提取数据并返回
		for (int i = 0; next <= end; next++, i++) {
			MessageExt input = msgs.get(i);
			Entry.Builder builder = Entry.newBuilder();
			try {
				builder.mergeFrom(input.getBody(), 0, input.getBody().length);
			} catch (InvalidProtocolBufferException e) {
				throw new CanalStoreException("builder.mergeFrom failure.", e);
			}
			// change logfileoffset => queueoffset logfileName => topic
			builder.getHeaderBuilder().setLogfileOffset(input.getQueueOffset()).setLogfileName(destination);
			Entry entry = builder.build();
			if(logger.isDebugEnabled())logger.debug("{}",entry);
			InetSocketAddress sa = null;
			Long slaveId = -1l;
			if (StringUtils.isNotEmpty(input.getTags())) {
				String[] tags = StringUtils.split(input.getTags(), "|");
				if (tags != null && tags.length > 1) {
					slaveId = Long.valueOf(tags[1]);
					sa = INET_SOCKETADDR.get(tags[0]);
					if(sa == null){
						String[] address = StringUtils.split(tags[0], ":");
						if (address.length > 1){
							sa = new InetSocketAddress(address[0], Integer.valueOf(address[1]));
							INET_SOCKETADDR.put(tags[0], sa);
						}
					}
				}
			}
			Event event = new Event(new LogIdentity(sa, slaveId), entry);
			if (ddlIsolation && isDdl(entry.getHeader().getEventType())) {
				// 如果是ddl隔离，直接返回
				if (entrys.size() == 0) {
					entrys.add(event);// 如果没有DML事件，加入当前的DDL事件
					end = next; // 更新end为当前
				} else {
					// 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
					end = next - 1; // next-1一定大于current，不需要判断
				}
				break;
			} else {
				entrys.add(event);
			}
		}

		PositionRange<LogPosition> range = new PositionRange<LogPosition>();
		result.setPositionRange(range);

		range.setStart(CanalEventUtils.createPosition(entrys.get(0)));
		range.setEnd(CanalEventUtils.createPosition(entrys.get(result.getEvents().size() - 1)));
		// 记录一下是否存在可以被ack的点
		for (int i = entrys.size() - 1; i >= 0; i--) {
			Event event = entrys.get(i);
			if (CanalEntry.EntryType.TRANSACTIONBEGIN == event.getEntry().getEntryType()
					|| CanalEntry.EntryType.TRANSACTIONEND == event.getEntry().getEntryType()
					|| isDdl(event.getEntry().getHeader().getEventType())) {
				// 将事务头/尾设置可被为ack的点
				range.setAck(CanalEventUtils.createPosition(event));
				break;
			}
		}

		if (getSequence.compareAndSet(current, end)) {
			return result;
		} else {
			return new Events<Event>();
		}
	}

	public LogPosition getFirstPosition() throws CanalStoreException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		//long firstSeqeuence = ackSequence.get();
		try {
			Long offset = null;
			try {
				offset = mqconsumer.getDefaultMQPullConsumerImpl().fetchConsumeOffset(mqMsgqueue, false);
			} catch (MQClientException e) {
				throw new CanalStoreException("getMessageQueueOffset failure.", e);
			}
			LogPosition position = createPosition(offset);
			Events<Event> event = doGet(position, 2, 3000);
			if (event.getEvents().size() > 0)
				position = CanalEventUtils.createPosition(event.getEvents().get(0), event.getEvents().size() > 1);

			// to be corrected
			return position;
		} finally {
			lock.unlock();
		}
	}

	public LogPosition getLatestPosition() throws CanalStoreException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		long firstSeqeuence = ackSequence.get();
		try {
			Long offset = null;
			try {
				offset = mqconsumer.getDefaultMQPullConsumerImpl().maxOffset(mqMsgqueue);
			} catch (MQClientException e) {
				throw new CanalStoreException("getMessageQueueOffset failure.", e);
			}
			LogPosition position = createPosition(offset);
			Events<Event> event = doGet(position, 2, 3000);
			if (event.getEvents().size() > 0)
				position = CanalEventUtils.createPosition(event.getEvents().get(0), firstSeqeuence >= offset);
			return position;
		} finally {
			lock.unlock();
		}
	}

	public void ack(Position position) throws CanalStoreException {
		cleanUntil(position);
	}

	public void rollback() throws CanalStoreException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			getSequence.set(ackSequence.get());
		} finally {
			lock.unlock();
		}
	}

	private boolean isDdl(EventType type) {
		return type == EventType.ALTER || type == EventType.CREATE || type == EventType.ERASE
				|| type == EventType.RENAME || type == EventType.TRUNCATE || type == EventType.CINDEX
				|| type == EventType.DINDEX;
	}

	@Override
	public void cleanUntil(Position position) throws CanalStoreException {
		// nothing
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			long sequence = ackSequence.get();
			long next = ((LogPosition) position).getPostion().getPosition();

			if (ackSequence.compareAndSet(sequence, next)) {// 避免并发ack
				try {
					mqconsumer.updateConsumeOffset(mqMsgqueue, next);
				} catch (MQClientException e) {
					ackSequence.compareAndSet(next, sequence);
					throw new CanalStoreException("mqconsumer.updateConsumeOffset failure.", e);
				}
				return;
			}

			throw new CanalStoreException("no match ack position" + position.toString());
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void cleanAll() throws CanalStoreException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			getSequence.set(INIT_SQEUENCE);
			ackSequence.set(INIT_SQEUENCE);
		} finally {
			lock.unlock();
		}
	}

	// ================ setter / getter ==================

	public void setNameSvrAddresses(String nameSvrAddresses) {
		this.nameSvrAddresses = nameSvrAddresses;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public void setPipelineId(Long pipelineId) {
		this.pipelineId = pipelineId;
	}

	public void setProducer(boolean producer) {
		this.producer = producer;
	}

	public void setConsumer(boolean consumer) {
		this.consumer = consumer;
	}

}
