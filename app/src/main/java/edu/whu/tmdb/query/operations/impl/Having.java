package edu.whu.tmdb.query.operations.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * HAVING clause evaluation for grouped SELECT results and
 * GroupDeputy migration checks.  Expression evaluation is delegated
 * to {@link Where#execute(Expression, SelectResult)}.
 */
public class Having {

    /**
     * Filter a grouped {@link SelectResult} by the HAVING clause
     * in {@code plainSelect}.  When there is no HAVING clause the
     * result is returned unchanged.
     */
    public SelectResult having(PlainSelect plainSelect, SelectResult selectResult)
            throws TMDBException, IOException {
        Expression havingExpr = plainSelect.getHaving();
        if (havingExpr == null) return selectResult;

        // Record original sorted order (from groupByElicit) before Where
        // scrambles it via HashSet-based filtering.
        // Use IdentityHashMap because groupByElicit tuples share tupleId=0.
        java.util.IdentityHashMap<Tuple, Integer> origPos =
            new java.util.IdentityHashMap<>();
        TupleList origList = selectResult.getTpl();
        for (int i = 0; i < origList.tuplelist.size(); i++) {
            origPos.put(origList.tuplelist.get(i), i);
        }

        Where where = new Where();
        where.execute(havingExpr, selectResult);
        retainSelectedGroups(selectResult, origList, selectResult.getTpl());

        // Restore sorted order: sort surviving tuples by original position
        selectResult.getTpl().tuplelist.sort(
            (a, b) -> Integer.compare(
                origPos.getOrDefault(a, Integer.MAX_VALUE),
                origPos.getOrDefault(b, Integer.MAX_VALUE)));

        return selectResult;
    }

    /**
     * Evaluate a HAVING expression against a {@link SelectResult}.
     * Delegates to {@link Where#execute(Expression, SelectResult)}.
     */
    public SelectResult execute(Expression havingExpr, SelectResult selectResult)
            throws TMDBException, IOException {
        return new Where().execute(havingExpr, selectResult);
    }

    /**
     * Check whether a single aggregated tuple satisfies the HAVING
     * clause of the given deputy class.  Used by
     * {@link GroupDeputyHelper} during migration.
     *
     * @return {@code true} when there is no HAVING clause or the
     *         tuple passes the HAVING check.
     */
    public static boolean satisfiesHaving(int deputyClassId, Tuple aggregatedTuple)
            throws TMDBException {
        Expression havingExpr = extractHaving(deputyClassId);
        if (havingExpr == null) return true;

        SelectResult singleResult = buildSingleTupleResult(deputyClassId, aggregatedTuple);
        try {
            SelectResult filtered = new Where().execute(havingExpr, singleResult);
            return !filtered.getTpl().tuplelist.isEmpty();
        } catch (IOException e) {
            return true;
        }
    }

    // ---- internal helpers ----

    /** Parse the deputy rule SQL and extract the HAVING expression. */
    static Expression extractHaving(int deputyClassId) {
        String sql = DeputyUtils.getRule(deputyClassId, 0);
        if (sql.isEmpty()) return null;

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(sql.getBytes());
            net.sf.jsqlparser.statement.select.Select select =
                (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(bais);
            if (select.getSelectBody() instanceof PlainSelect) {
                return ((PlainSelect) select.getSelectBody()).getHaving();
            }
        } catch (JSQLParserException ignored) {
        }
        return null;
    }

    /**
     * Build a single-tuple {@link SelectResult} so that
     * {@link Where} can evaluate a HAVING expression against it.
     * {@code alias} stores the original aggregate expression (e.g.
     * {@code "COUNT(id)"}) so that {@code Formula} can resolve
     * function-style references in the HAVING clause.
     */
    static SelectResult buildSingleTupleResult(int deputyClassId, Tuple tuple)
            throws TMDBException {
        MemConnect memConnect = MemConnect.getInstance(MemManager.getInstance());
        int attrNum = memConnect.getClassAttrnum(deputyClassId);

        SelectResult result = new SelectResult(attrNum);
        TupleList tl = new TupleList();
        tl.addTuple(tuple);
        result.setTpl(tl);

        for (int i = 0; i < attrNum; i++) {
            for (SwitchingTableItem sti : MemConnect.getSwitchingTableList()) {
                if (sti.deputyId != deputyClassId || sti.deputyAttrId != i) continue;
                result.getClassName()[i] = memConnect.getClassName(sti.oriId);
                result.getAttrname()[i] = sti.deputyAttr;
                result.getType()[i] = "double";
                result.getAttrid()[i] = i;
                if (!sti.oriAttr.equals(sti.deputyAttr)) {
                    result.getAlias()[i] =
                        sti.rule.toUpperCase() + "(" + sti.oriAttr + ")";
                } else {
                    result.getAlias()[i] = sti.deputyAttr;
                }
                break;
            }
        }
        return result;
    }

    /**
     * Remove group-map entries for groups that were eliminated
     * by the HAVING filter. We compare by group KEY (the first
     * column of each aggregated row) because the synthetic tuples
     * from groupByElicit all share tupleId=0.
     */
    private void retainSelectedGroups(SelectResult result,
                                      TupleList original, TupleList filtered) {
        HashMap<Object, ArrayList<Integer>> oldMap = result.getGroupMap();
        if (oldMap == null) return;

        HashSet<Object> survivingKeys = new HashSet<>();
        for (Tuple t : filtered.tuplelist) {
            survivingKeys.add(t.tuple.length > 0 ? t.tuple[0] : null);
        }

        Iterator<Map.Entry<Object, ArrayList<Integer>>> it =
            oldMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Object, ArrayList<Integer>> entry = it.next();
            if (!survivingKeys.contains(entry.getKey())) it.remove();
        }
        result.setGroupMap(oldMap);
    }
}
