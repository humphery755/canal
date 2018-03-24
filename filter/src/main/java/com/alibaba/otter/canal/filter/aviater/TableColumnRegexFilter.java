package com.alibaba.otter.canal.filter.aviater;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.filter.exception.CanalFilterException;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.google.protobuf.InvalidProtocolBufferException;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

/**
 * 基于aviater进行tableName.columnName匹配的过滤算法
 * 
 * @author humphery 2017-7-20 下午06:01:34
 */
public class TableColumnRegexFilter implements CanalEventFilter<CanalEntry.Entry> {
    private static final Logger    logger                        = LoggerFactory.getLogger(TableColumnRegexFilter.class);

    private static final String        SPLIT             = ",";
    private static final String        PATTERN_SPLIT     = "|";
    private static final String        FILTER_EXPRESSION = "regex(pattern,target)";
    private static final RegexFunction regexFunction     = new RegexFunction();
    private final Expression           exp               = AviatorEvaluator.compile(FILTER_EXPRESSION, true);
    static {
        AviatorEvaluator.addFunction(regexFunction);
    }

    private static final Comparator<String> COMPARATOR   = new StringComparator();

    final private String                    pattern;
    final private boolean                   defaultEmptyValue;
    final private Map<String, Set<String>>  columnMaping = new HashMap();

    public TableColumnRegexFilter(String pattern){
        this(pattern, true);
    }

    /**
     * 
     * @param pattern schema.table1:column1|column2|column3,schema.table2:column1|column2|column3
     * @param defaultEmptyValue
     */
    public TableColumnRegexFilter(String pattern, boolean defaultEmptyValue){
        this.defaultEmptyValue = defaultEmptyValue;
        List<String> list = null;
        if (StringUtils.isEmpty(pattern)) {
            list = new ArrayList<String>();
        } else {
            String[] ss = StringUtils.split(pattern, SPLIT);
            list = Arrays.asList(ss);
        }
        List<String> tmpList = new ArrayList<String>();
        for (String s : list) {
            String p = s;
            int start = s.indexOf(":");
            if (start > 0) {
                p = StringUtils.substring(s, 0, start);
                s = StringUtils.substring(s, start + 1);
                s = s.toLowerCase();
                String[] ss = StringUtils.split(s, PATTERN_SPLIT);
                if (ss != null && ss.length > 0) {
                    Set<String> cols = new HashSet(Arrays.asList(ss));
                    columnMaping.put(p, cols);
                }
            }
            tmpList.add(p);
        }

        // 对pattern按照从长到短的排序
        // 因为 foo|foot 匹配 foot 会出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot
        // 的长度不一样
        Collections.sort(tmpList, COMPARATOR);
        // 对pattern进行头尾完全匹配
        tmpList = completionPattern(tmpList);
        this.pattern = StringUtils.join(tmpList, PATTERN_SPLIT);
    }

    public boolean filter(CanalEntry.Entry entry) throws CanalFilterException {
        if (entry == null) {
            return defaultEmptyValue;
        }
        String filtered = entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
        boolean res = filter(filtered);
        if (!res) {
            return res;
        }
        Set<String> cols = columnMaping.get(filtered);
        if (cols == null) {
            return res;
        }
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            if (rowChange.getIsDdl()) {
                return false;
            }
            
            List<RowData> rowDatas = rowChange.getRowDatasList();
            switch (rowChange.getEventType()) {
                case DELETE:
                         logger.debug("filter name[{}.{}] entry : {}:{}",
                             filtered,"EventType-DELETE",
                             entry.getHeader().getLogfileName(),
                             entry.getHeader().getLogfileOffset());
                    return true;
                default:
                    for (RowData rd : rowDatas) {
                        List<Column> cList = rd.getAfterColumnsList();
                        for (Column c : cList) {
                            if (c.getUpdated() && cols.contains(c.getName().toLowerCase())){
                                logger.debug("filter name[{}.{}] entry : {}:{}",
                                    filtered,c.getName(),
                                    entry.getHeader().getLogfileName(),
                                    entry.getHeader().getLogfileOffset());
                                return true;
                            }
                        }
                    }
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            new CanalFilterException(e);
        }
        return false;
    }

    public boolean filter(String filtered) throws CanalFilterException {
        if (StringUtils.isEmpty(pattern)) {
            return defaultEmptyValue;
        }

        if (StringUtils.isEmpty(filtered)) {
            return defaultEmptyValue;
        }

        Map<String, Object> env = new HashMap<String, Object>();
        env.put("pattern", pattern);
        env.put("target", filtered.toLowerCase());
        return (Boolean) exp.execute(env);
    }

    /**
     * 修复正则表达式匹配的问题，因为使用了 oro 的 matches，会出现：
     * 
     * <pre>
     * foo|foot 匹配 foot 出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot 的长度不一样
     * </pre>
     * 
     * 因此此类对正则表达式进行了从长到短的排序
     * 
     * @author zebin.xuzb 2012-10-22 下午2:02:26
     * @version 1.0.0
     */
    private static class StringComparator implements Comparator<String> {

        @Override
        public int compare(String str1, String str2) {
            if (str1.length() > str2.length()) {
                return -1;
            } else if (str1.length() < str2.length()) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    /**
     * 修复正则表达式匹配的问题，即使按照长度递减排序，还是会出现以下问题：
     * 
     * <pre>
     * foooo|f.*t 匹配 fooooot 出错，原因是 fooooot 匹配了 foooo 之后，会将 fooo 和数据进行匹配，但是 foooo 的长度和 fooooot 的长度不一样
     * </pre>
     * 
     * 因此此类对正则表达式进行头尾完全匹配
     * 
     * @author simon
     * @version 1.0.0
     */

    private List<String> completionPattern(List<String> patterns) {
        List<String> result = new ArrayList<String>();
        for (String pattern : patterns) {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("^");
            stringBuffer.append(pattern);
            stringBuffer.append("$");
            result.add(stringBuffer.toString());
        }
        return result;
    }

}
