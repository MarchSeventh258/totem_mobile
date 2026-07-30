package edu.whu.tmdb.query.operations.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
 * Centralized GroupDeputy reconciliation logic.
 * <p>
 * When a source class has one or more GroupDeputy deputies, every
 * INSERT / UPDATE / DELETE on the source class must update the
 * aggregate values in the deputy.  This helper encapsulates the
 * query→aggregate→HAVING→create-or-update cycle so the Impl
 * classes share a single implementation.
 */
final class GroupDeputyHelper {

    private GroupDeputyHelper() {}

    // ---- public API ----

    /**
     * Returns the source attribute name that serves as the grouping
     * key for the given deputy relationship.
     */
    static String getGroupAttr(int originClassId, int deputyClassId) {
        for (SwitchingTableItem sti : MemConnect.getSwitchingTableList()) {
            if (sti.oriId == originClassId && sti.deputyId == deputyClassId) {
                // The first entry whose oriAttr equals deputyAttr is
                // the grouping column (non-aggregated).
                if (sti.oriAttr.equals(sti.deputyAttr)) {
                    return sti.oriAttr;
                }
            }
        }
        return "";
    }

    /**
     * Reconcile the GroupDeputy for a single group value after the
     * source class has changed.
     * <ol>
     *   <li>Query source tuples belonging to the group</li>
     *   <li>If the group is empty → delete the deputy tuple</li>
     *   <li>Aggregate the group</li>
     *   <li>If HAVING is not satisfied → delete the deputy tuple</li>
     *   <li>Create or update the deputy tuple and rebuild BiPointers</li>
     * </ol>
     */
    static void reconcileGroup(int originClassId, int deputyClassId,
                               Object groupObj) throws TMDBException, IOException {
        MemConnect memConnect = MemConnect.getInstance(MemManager.getInstance());
        String groupAttr = getGroupAttr(originClassId, deputyClassId);
        if (groupAttr.isEmpty()) return;

        // 1. Query current source tuples for this group
        String originName = memConnect.getClassName(originClassId);
        TupleList sourceGroup;
        try {
            String sql = "select * from " + originName + " where "
                + groupAttr + " = " + DeputyUtils.sqlLiteral(groupObj);
            ByteArrayInputStream bais = new ByteArrayInputStream(sql.getBytes());
            Statement stmt = CCJSqlParserUtil.parse(bais);
            Transaction tx = Transaction.getInstance();
            SelectResult sr = tx.query("", -1, stmt);
            sourceGroup = sr.getTpl();
        } catch (JSQLParserException e) {
            return;
        }

        // 2. Find existing deputy tuple
        Tuple existing = findDeputyTuple(originClassId, deputyClassId, groupObj);

        // 3. If no source tuples remain, delete the deputy
        if (sourceGroup.tuplenum == 0) {
            if (existing != null) deleteDeputyTuple(existing);
            return;
        }

        // 4. Aggregate
        Tuple aggregated = aggregate(originClassId, deputyClassId,
            groupObj, sourceGroup);

        // 5. Check HAVING
        if (!Having.satisfiesHaving(deputyClassId, aggregated)) {
            if (existing != null) deleteDeputyTuple(existing);
            return;
        }

        // 6. Create or update
        String deputyName = memConnect.getClassName(deputyClassId);
        int attrNum = memConnect.getClassAttrnum(deputyClassId);

        if (existing == null) {
            int newId = MemConnect.getObjectTable().maxTupleId++;
            MemConnect.getObjectTableList().add(
                new ObjectTableItem(deputyClassId, newId));

            aggregated.tupleSize = attrNum;
            aggregated.tupleId = newId;
            aggregated.classId = deputyClassId;
            aggregated.delete = false;

            memConnect.InsertTuple(aggregated);
            rebuildBiPointers(originClassId, deputyClassId, newId, sourceGroup);
            sortDeputyTuples(deputyClassId);
        } else {
            existing.tuple = aggregated.tuple;
            memConnect.UpateTuple(existing, existing.getTupleId());
            rebuildBiPointers(originClassId, deputyClassId, existing.getTupleId(), sourceGroup);
        }
    }

    /** Sort deputy-class tuples by group key so SELECT * returns them in GROUP BY order. */
    private static void sortDeputyTuples(int deputyClassId) {
        MemConnect memConnect = MemConnect.getInstance(MemManager.getInstance());
        java.util.List<ObjectTableItem> objList = MemConnect.getObjectTableList();

        // Collect deputy-class entries with their group keys
        java.util.List<java.util.AbstractMap.SimpleEntry<ObjectTableItem, Comparable>> items =
            new java.util.ArrayList<>();
        for (ObjectTableItem oti : objList) {
            if (oti.classid != deputyClassId) continue;
            Tuple t = memConnect.GetTuple(oti.tupleid);
            Comparable key = (t != null && t.tuple.length > 0 && t.tuple[0] instanceof Comparable)
                ? (Comparable) t.tuple[0] : "";
            items.add(new java.util.AbstractMap.SimpleEntry<>(oti, key));
        }

        // Sort by group key
        items.sort((a, b) -> {
            Comparable ka = a.getValue();
            Comparable kb = b.getValue();
            if (ka == null && kb == null) return 0;
            if (ka == null) return -1;
            if (kb == null) return 1;
            @SuppressWarnings("unchecked")
            int cmp = ka.compareTo(kb);
            return cmp;
        });

        // Reorder objList: remove deputy entries then re-add in sorted order
        java.util.Iterator<ObjectTableItem> it = objList.iterator();
        while (it.hasNext()) {
            if (it.next().classid == deputyClassId) it.remove();
        }
        for (java.util.AbstractMap.SimpleEntry<ObjectTableItem, Comparable> e : items) {
            objList.add(e.getKey());
        }
    }

    // ---- internal helpers ----

    /**
     * Compute a single aggregated tuple from the source group.
     * The result layout matches the deputy class columns:
     * [{@code groupObj}, {@code agg1}, {@code agg2}, …].
     */
    private static Tuple aggregate(int originClassId, int deputyClassId,
                                   Object groupObj, TupleList sourceGroup)
            throws TMDBException {
        MemConnect memConnect = MemConnect.getInstance(MemManager.getInstance());

        // Collect aggregate-function columns from SwitchingTable
        List<String> funcNames = new ArrayList<>();
        List<Integer> pIndexes = new ArrayList<>();

        for (SwitchingTableItem sti : MemConnect.getSwitchingTableList()) {
            if (sti.oriId != originClassId || sti.deputyId != deputyClassId) continue;
            if (!sti.oriAttr.equals(sti.deputyAttr)) {
                funcNames.add(sti.rule);
                if ("*".equals(sti.oriAttr)) {
                    pIndexes.add(-1); // COUNT(*) — count all tuples
                } else {
                    pIndexes.add(memConnect.getAttrid(originClassId, sti.oriAttr));
                }
            }
        }

        Tuple result = new Tuple();
        result.tuple = new Object[funcNames.size() + 1];
        result.tuple[0] = groupObj;

        for (int idx = 0; idx < funcNames.size(); idx++) {
            int pi = pIndexes.get(idx);
            String func = funcNames.get(idx);
            if (pi == -1) {
                // COUNT(*): count all source tuples, not a specific column
                result.tuple[idx + 1] = (double) sourceGroup.tuplenum;
            } else {
                List<Double> values = new ArrayList<>();
                for (Tuple t : sourceGroup.tuplelist) {
                    if (t.tuple[pi] != null) {
                        values.add(Double.parseDouble(String.valueOf(t.tuple[pi])));
                    }
                }
                result.tuple[idx + 1] = solveAggregate(func, values);
            }
        }
        return result;
    }

    private static double solveAggregate(String funcName, List<Double> values) {
        switch (funcName.toLowerCase()) {
            case "avg":
                return values.stream().mapToDouble(Double::doubleValue)
                    .average().orElse(0);
            case "min":
                return values.stream().mapToDouble(Double::doubleValue)
                    .min().orElse(0);
            case "max":
                return values.stream().mapToDouble(Double::doubleValue)
                    .max().orElse(0);
            case "count": return values.size();
            case "sum":
                return values.stream().mapToDouble(Double::doubleValue).sum();
            default:  return 0;
        }
    }

    /** Find the existing deputy tuple for a group, or {@code null}. */
    private static Tuple findDeputyTuple(int originClassId, int deputyClassId,
                                         Object groupObj) {
        MemConnect memConnect = MemConnect.getInstance(MemManager.getInstance());
        TupleList deputyTpl = memConnect.getTupleList(deputyClassId);
        for (Tuple t : deputyTpl.tuplelist) {
            if (t.tuple.length > 0
                    && String.valueOf(t.tuple[0]).equals(String.valueOf(groupObj))) {
                return t;
            }
        }
        return null;
    }

    /** Delete a deputy tuple and all of its BiPointer entries. */
    private static void deleteDeputyTuple(Tuple deputyTuple) {
        MemConnect memConnect = MemConnect.getInstance(MemManager.getInstance());
        int tid = deputyTuple.getTupleId();

        memConnect.DeleteTuple(tid);

        Iterator<ObjectTableItem> objIt = MemConnect.getObjectTableList().iterator();
        while (objIt.hasNext()) {
            if (objIt.next().tupleid == tid) { objIt.remove(); break; }
        }

        Iterator<BiPointerTableItem> bpIt =
            MemConnect.getBiPointerTableList().iterator();
        while (bpIt.hasNext()) {
            if (bpIt.next().deputyobjectid == tid) bpIt.remove();
        }
    }

    /**
     * Remove existing BiPointers for the deputy and create fresh
     * ones linking every source-group tuple to the deputy.
     */
    private static void rebuildBiPointers(int originClassId, int deputyClassId,
                                          int deputyTupleId, TupleList sourceGroup) {
        // Remove old pointers
        Iterator<BiPointerTableItem> it =
            MemConnect.getBiPointerTableList().iterator();
        while (it.hasNext()) {
            BiPointerTableItem bpi = it.next();
            if (bpi.deputyid == deputyClassId && bpi.deputyobjectid == deputyTupleId)
                it.remove();
        }

        // Add new pointers
        for (Tuple src : sourceGroup.tuplelist) {
            MemConnect.getBiPointerTableList().add(
                new BiPointerTableItem(originClassId, src.getTupleId(),
                    deputyClassId, deputyTupleId));
        }
    }
}
