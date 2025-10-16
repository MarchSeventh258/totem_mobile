package edu.whu.tmdb.query.operations.impl;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Insert;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyRuleTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

public class InsertImpl implements Insert {
    private MemConnect memConnect;

    ArrayList<Integer> tupleIdList = new ArrayList<>();

    public InsertImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public ArrayList<Integer> insert(Statement stmt) throws TMDBException, IOException {
        net.sf.jsqlparser.statement.insert.Insert insertStmt = (net.sf.jsqlparser.statement.insert.Insert) stmt;
        Table table = insertStmt.getTable();        // 解析insert对应的表
        List<String> attrNames = new ArrayList<>(); // 解析插入的字段名
        if (insertStmt.getColumns() == null){
            attrNames = memConnect.getColumns(table.getName());
        }
        else{
            int insertColSize = insertStmt.getColumns().size();
            for (int i = 0; i < insertColSize; i++) {
                attrNames.add(insertStmt.getColumns().get(i).getColumnName());
            }
        }

        // 对应含有子查询的插入语句
        SelectImpl select = new SelectImpl();
        SelectResult selectResult = select.select(insertStmt.getSelect());

        // tuplelist存储需要插入的tuple部分
        TupleList tupleList = selectResult.getTpl();
        execute(table.getName(), attrNames, tupleList);
        return tupleIdList;
    }

    /**
     * @param tableName 表名/类名
     * @param columns 表/类所具有的属性列表
     * @param tupleList 要插入的元组列表
     */
    public void execute(String tableName, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int classId = memConnect.getClassId(tableName);         // 类id
        int attrNum = memConnect.getClassAttrnum(tableName);    // 属性的数量
        int[] attrIdList = memConnect.getAttridList(classId, columns);
        int len = tupleList.tuplelist.size();// 插入的属性对应的attrid列表
        for (Tuple tuple : tupleList.tuplelist) {
            if (tuple.tuple.length != columns.size()){
                throw new TMDBException(/*"Insert error: columns size doesn't match tuple size"*/);
            }
            tupleIdList.add(insertOne(classId, columns, tuple, attrNum, attrIdList));
        }
    }

    /**
     * @param classId 表/类id
     * @param columns 表/类所具有的属性列表
     * @param tupleList 要插入的元组列表
     */
    public void execute(int classId, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int attrNum = memConnect.getClassAttrnum(classId);
        int[] attrIdList = memConnect.getAttridList(classId, columns);
        for (Tuple tuple : tupleList.tuplelist) {
            if (tuple.tuple.length != columns.size()){
                throw new TMDBException(/*"Insert error: columns size doesn't match tuple size"*/);
            }
            tupleIdList.add(insertOne(classId, columns, tuple, attrNum, attrIdList));
        }
    }

    /**
     * @param classId 要插入的类id
     * @param columns 要插入类的属性名列表
     * @param tuple 要insert的元组tuple
     * @return 新插入元组的tuple id
     */
    public int executeOne(int classId, List<String> columns, Tuple tuple) throws TMDBException, IOException {
        int attrNum = memConnect.getClassAttrnum(classId);
        int[] attridList = memConnect.getAttridList(classId, columns);

        if (tuple.tuple.length != columns.size()){
            throw new TMDBException(/*"Insert error: columns size doesn't match tuple size"*/);
        }
        int tupleId = insertOne(classId, columns, tuple, attrNum, attridList);
        tupleIdList.add(tupleId);
        return tupleId;
    }

    public String deputyquery(int classid,int deputyid)
    {
        for(SwitchingTableItem switchingTableItem : MemConnect.getSwitchingTableList())
        {
            if(switchingTableItem.oriId ==classid && switchingTableItem.deputyId == deputyid )
                return switchingTableItem.rule;
        }
        return "";

    }

    public int repeattuple(int classid,Tuple checktuple)
    {     int num = 0;int tuplelen = checktuple.tupleSize;
            TupleList classtuplelisty = memConnect.getTupleList(classid);
            int  flag = 1;
            for(Tuple temptuple : classtuplelisty.tuplelist )
            {
                flag = 1 ;
                for(int i=0;i<tuplelen;i++)
                {
                    if(!temptuple.tuple[i].equals(checktuple.tuple[i]))
                    {
                        flag = 0;
                        break;
                    }
                }
                if(flag == 1)
                    num++;

            }
        return num;
    }

    public static String getdeputyrule(int deputyclassid, int index) {
        for(DeputyTableItem tempitem : MemConnect.getDeputyTableList())
        {
            if(tempitem.deputyid == deputyclassid)
                for(DeputyRuleTableItem tempruleitem : MemConnect.getDeputyRuleTableList())
                {
                    if(tempruleitem.ruleid == tempitem.ruleid)
                    return tempruleitem.deputyrule[index];
                }
        }
        return "";
    }

    public TupleList except2(TupleList ltpl, TupleList rtpl) {
        TupleList tupleList=new TupleList();
        List<Tuple> set=new ArrayList<>();
        for(Tuple tuple:ltpl.tuplelist){
            set.add(tuple);
        }
        //如果2中含有tuple，则在结果集合中移除
        int len = set.size();
        for(Tuple tuple:rtpl.tuplelist){
            for(int i = 0;i<len;i++)
            {
                if(tuple.equals(set.get(i)))
                {
                    set.remove(i);len--;
                    break;
                }
            }

        }
        tupleList.tuplelist=set;
        tupleList.tuplenum=set.size();

        return tupleList;
    }

    public TupleList except(TupleList ltpl, TupleList rtpl) {
        TupleList tupleList=new TupleList();
        List<Tuple> set=new ArrayList<>();
        for(Tuple tuple:ltpl.tuplelist){
            set.add(tuple);
        }
        //如果2中含有tuple，则在结果集合中移除
        int len = set.size();
        for(Tuple tuple:rtpl.tuplelist){
            for(int i = 0;i<len;i++)
            {
                if(Arrays.toString(tuple.tuple).equals(Arrays.toString(set.get(i).tuple)))
                {
                    len --; set.remove(i);break;
                }
            }

        }
        tupleList.tuplelist=set;
        tupleList.tuplenum=set.size();

        return tupleList;
    }

    public String getDeputyAttr(int classid, int deputyid) {
        for(SwitchingTableItem switchingTableItem : MemConnect.getSwitchingTableList()) {
            if(switchingTableItem.oriId ==classid && switchingTableItem.deputyId == deputyid )
                return switchingTableItem.oriAttr;
        }
        return "";
    }

    /**
     * @param classId 插入表/类对应的id
     * @param columns 表/类所具有的属性名列表（来自insert语句）
     * @param tuple 要插入的元组
     * @param attrNum 元组包含的属性数量（系统表中获取）
     * @param attrId 插入属性对应的attrId列表（根据insert的属性名，系统表中获取）
     * @return 新插入属性的tuple id
     */
    private Integer insertOne(int classId, List<String> columns, Tuple tuple, int attrNum, int[] attrId) throws TMDBException, IOException {
        // 1.直接在对应类中插入tuple
        // 1.1 获取新插入元组的id
        int tupleid = MemConnect.getObjectTable().maxTupleId++;

        // 1.2 将tuple转换为可插入的形式
        Object[] temp = new Object[attrNum];
        for (int i = 0; i < attrId.length; i++) {
            temp[attrId[i]] = tuple.tuple[i];
        }
        tuple.setTuple(tuple.tuple.length, tupleid, classId,tuple.tupleIds, temp);
        // 1.3 元组插入操作
        memConnect.InsertTuple(tuple);
        MemConnect.getObjectTableList().add(new ObjectTableItem(classId, tupleid));

        // 2.找到所有的代理类，进行递归插入
        // 2.1 找到源类所有的代理类
        ArrayList<Integer> DeputyIdList = memConnect.getDeputyIdList(classId);

        // 2.2 将元组转换为代理类应有的形式
        if (!DeputyIdList.isEmpty()) {
            for (int deputyClassId : DeputyIdList) {
                String stmt = getdeputyrule(deputyClassId,0);
                Transaction transaction = Transaction.getInstance();    // 创建一个事务实例
                SelectResult selectResult = null;
                String deputyname = memConnect.getClassName(deputyClassId);
                if(Objects.equals(deputyquery(classId, deputyClassId), "joindeputy")) //处理join代理
                {   
                    try {
                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(stmt.getBytes());
                        Statement selectstmt = CCJSqlParserUtil.parse(byteArrayInputStream);
                        TupleList inserttupleList = new TupleList();
                        inserttupleList.addTuple(tuple);
                        selectResult = transaction.joinquery("", -1, selectstmt,inserttupleList);
                    }catch (JSQLParserException e) {

                        System.out.println("syntax error");
                    }
                    TupleList newDeputyTpl = selectResult.getTpl();
                    String [] classNames = selectResult.getClassName();
                    HashSet<String> processedClassNames = new HashSet<>();
                    if (newDeputyTpl.tuplelist.size() != 0) {
                        for (String className : classNames) { //hash表对classid去重
                            if (processedClassNames.contains(className)) {
                                continue;
                            }
                            processedClassNames.add(className);   
                        }
                        for (Tuple inserttuple : newDeputyTpl.tuplelist) {
                            int inserttupleid = executeOne(deputyClassId, memConnect.getColumns(deputyname), inserttuple);
                            // 创建 BiPointerTableItem 对象并设置相关
                            Map<String, Integer> classTupleMap = new HashMap<>();
                            for (int j = 0; j < classNames.length; j++) {
                            classTupleMap.put(classNames[j], inserttuple.tupleIds[j]);
                            }
                            for(String className : processedClassNames){
                                int originId = memConnect.getClassId(className);
                                BiPointerTableItem biPointerItem = new BiPointerTableItem(originId, classTupleMap.get(className), deputyClassId, inserttupleid);
                                // 使用 MemConnect.getBiPointerTableList().add() 插入 BiPointerTable
                                MemConnect.getBiPointerTableList().add(biPointerItem);
                            }
                        }
                    }
                }
                else if (Objects.equals(deputyquery(classId, deputyClassId), "groupdeputy")) {
                    // 获得分组依据，聚合函数名以及聚合值的索引下标
                    String groupAttr = getDeputyAttr(classId, deputyClassId);
                    int groupAttrIdx = memConnect.getAttrid(classId, groupAttr);
                    Object groupObj = temp[groupAttrIdx];
                    ArrayList<String> funcNames = new ArrayList<>();
                    ArrayList<Integer> pIndexs = new ArrayList<>();
                    for (SwitchingTableItem sti : MemConnect.getSwitchingTableList()) {
                        if(classId == sti.oriId && deputyClassId == sti.deputyId) {
                            if(!Objects.equals(sti.oriAttr, sti.deputyAttr)) { // 聚合列的源列名是不带聚合函数的，代理列名是带聚合函数的
                                funcNames.add(sti.rule);
                                pIndexs.add(memConnect.getAttrid(classId, sti.oriAttr));
                            }
                        }
                    }

                    // 执行对分组依据的查询（如果有索引，那么效率为log复杂度）
                    try {
                        String sql = "select * from " + memConnect.getClassName(classId) + " where " + groupAttr + " = " + groupObj.toString() + ";";
                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
                        Statement selectstmt = CCJSqlParserUtil.parse(byteArrayInputStream);
                        selectResult = transaction.query("", -1, selectstmt);
                    }catch (JSQLParserException e) {
                        System.out.println("syntax error");
                    }
                    TupleList changedList = selectResult.getTpl();

                    // 在这里已经得到了resultMap中的obg, list<tuple>，也就是groupobj和changedlist.tpl，再次对这些进行分组计算
                    SelectImpl selecttemp = new SelectImpl();
                    Tuple t = selecttemp.aggOne(groupObj, changedList, funcNames, pIndexs);

                    // 插入新的分组
                    if(changedList.tuplenum == 1) {
                        // 直接插入一条新记录，然后更新双指针表
                        int inserttupleid= executeOne(deputyClassId, memConnect.getColumns(deputyname), t);
                        MemConnect.getBiPointerTableList().add(new BiPointerTableItem(classId, tupleid, deputyClassId, inserttupleid));
                    }
                    // 插入已有的分组
                    else {
                        // 找到分组依据的代理对象id
                        TupleList oldDeputyTpl = memConnect.getTupleList(deputyClassId);
                        int groupObjDptId = -1;
                        Tuple oldt = null;
                        for(Tuple tempt : oldDeputyTpl.tuplelist) {
                            if(Objects.equals(tempt.tuple[0], groupObj)) {
                                groupObjDptId = tempt.tupleId;
                                oldt = tempt;
                            }
                        }
                        // 把新聚合结果给旧tuple
                        UpdateImpl updatetemp = new UpdateImpl();
                        updatetemp.updateOne(oldt, t);
                        // 插入双指针表
                        MemConnect.getBiPointerTableList().add(new BiPointerTableItem(classId, tupleid, deputyClassId, groupObjDptId));
                    }
                }
                else {
                    try {
                            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(stmt.getBytes());
                            Statement selectstmt = CCJSqlParserUtil.parse(byteArrayInputStream);
    
    
                            selectResult = transaction.query("", -1, selectstmt);
                        }catch (JSQLParserException e) {
    
                            System.out.println("syntax error");
                        }
    
                    TupleList oldDeputyTpl = memConnect.getTupleList(deputyClassId);
                    TupleList queryTpl = selectResult.getTpl();
                    TupleList insertTpl = new TupleList();
                    insertTpl = except2(queryTpl, oldDeputyTpl);
                    if (insertTpl.tuplelist.size() != 0) {
                        for (Tuple inserttuple : insertTpl.tuplelist) {
                            int inserttupleid = executeOne(deputyClassId, memConnect.getColumns(deputyname), inserttuple);
                            MemConnect.getBiPointerTableList().add(new BiPointerTableItem(classId, tupleid, deputyClassId, inserttupleid));
                        }
                    }
                }
                

            }

        }
        return tupleid;
    }

    /**
     * 获取源类属性列表->代理类属性列表的哈希映射列表（注：可能有的源类属性不在代理类中）
     * @param originClassId 源类的class id
     * @param deputyClassId 代理类的class id
     * @param originColumns 源类属性名列表
     * @return 源类属性列表->代理类属性列表的哈希映射列表
     */
    private HashMap<String, String> getAttrNameHashMap(int originClassId, int deputyClassId, List<String> originColumns) {
        HashMap<String, String> attrNameHashMap = new HashMap<>();
        for (SwitchingTableItem switchingTableItem : MemConnect.getSwitchingTableList()) {
            if (switchingTableItem.oriId != originClassId || switchingTableItem.deputyId != deputyClassId) {
                continue;
            }

            for (String originColumn : originColumns) {
                if (switchingTableItem.oriAttr.equals(originColumn)) {
                    attrNameHashMap.put(originColumn, switchingTableItem.deputyAttr);
                }
            }
        }
        return attrNameHashMap;
    }

    /**
     * 给定源类属性名列表，获取其代理类对应属性名列表（注：源类中有的属性可能不在代理类中）
     * @param attrNameHashMap 源类属性名->代理类属性名的哈希表
     * @param originColumns 源类属性名列表
     * @return 代理类属性名列表（注：源类中有的属性可能不在代理类中）
     */
    private List<String> getDeputyColumns(HashMap<String, String> attrNameHashMap, List<String> originColumns) {
        List<String> deputyColumns = new ArrayList<>();
        for (String originColumn : originColumns) {
            if (attrNameHashMap.containsKey(originColumn)){
                deputyColumns.add(attrNameHashMap.get(originColumn));
            }
        }
        return deputyColumns;
    }

    /**
     * 将插入源类的元组转换为插入代理类的元组
     * @param attrNameHashMap 源类属性名->代理类属性名的哈希表
     * @param originTuple 插入源类中的tuple
     * @param originColumns 源类属性名列表
     * @return 能够插入代理类的tuple
     */
    private Tuple getDeputyTuple(HashMap<String, String> attrNameHashMap, Tuple originTuple, List<String> originColumns) {
        Tuple deputyTuple = new Tuple();
        Object[] temp = new Object[attrNameHashMap.size()];
        int i = 0;
        for(String originColumn : originColumns){
            if (attrNameHashMap.containsKey(originColumn)){
                temp[i] = originTuple.tuple[originColumns.indexOf(originColumn)];
                i++;
            }
        }
        
        deputyTuple.tuple = temp;

        return deputyTuple;
    }
}
