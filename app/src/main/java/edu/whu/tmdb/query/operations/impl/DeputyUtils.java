package edu.whu.tmdb.query.operations.impl;

import java.util.Locale;

import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyRuleTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;

/**
 * Shared constants and utilities for deputy class operations.
 * Centralizes deputy type strings and rule lookups to eliminate
 * magic-string duplication across Impl classes.
 */
final class DeputyUtils {

    static final String SELECT_DEPUTY = "selectdeputy";
    static final String JOIN_DEPUTY    = "joindeputy";
    static final String UNION_DEPUTY   = "uniondeputy";
    static final String GROUP_DEPUTY   = "groupdeputy";
    static final String NON_STRICT     = "nonstrict";

    private DeputyUtils() {}

    // ---- Rule retrieval ----

    /** Returns the full deputy-rule array for a given deputy class. */
    static String[] getRuleArray(int deputyClassId) {
        for (DeputyTableItem dti : MemConnect.getDeputyTableList()) {
            if (dti.deputyid != deputyClassId) continue;
            for (DeputyRuleTableItem rti : MemConnect.getDeputyRuleTableList()) {
                if (rti.ruleid == dti.ruleid) return rti.deputyrule;
            }
        }
        return null;
    }

    /** Returns the {@code index}-th element of the deputy rule, or "" if absent. */
    static String getRule(int deputyClassId, int index) {
        String[] rule = getRuleArray(deputyClassId);
        if (rule == null || index < 0 || index >= rule.length) return "";
        return rule[index] == null ? "" : rule[index];
    }

    // ---- Type checks ----

    static boolean isType(int deputyClassId, String type) {
        return type.equalsIgnoreCase(getRule(deputyClassId, 1));
    }

    static boolean isGroupDeputy(int deputyClassId) {
        return isType(deputyClassId, GROUP_DEPUTY);
    }

    static boolean isSelectDeputy(int deputyClassId) {
        return isType(deputyClassId, SELECT_DEPUTY);
    }

    /** True when the deputy is a SelectDeputy created without a WHERE clause. */
    static boolean isNonStrictSelectDeputy(int deputyClassId) {
        return isSelectDeputy(deputyClassId)
                && NON_STRICT.equalsIgnoreCase(getRule(deputyClassId, 2));
    }

    /** True when the deputy is a SelectDeputy created with a WHERE clause. */
    static boolean isStrictSelectDeputy(int deputyClassId) {
        return isSelectDeputy(deputyClassId)
                && !NON_STRICT.equalsIgnoreCase(getRule(deputyClassId, 2));
    }

    /** True when {@code deputyClassId} is registered as a deputy of {@code originClassId}. */
    static boolean isDeputyOf(int originClassId, int deputyClassId) {
        for (DeputyTableItem dti : MemConnect.getDeputyTableList()) {
            if (dti.originid == originClassId && dti.deputyid == deputyClassId) return true;
        }
        return false;
    }

    // ---- Value helpers ----

    /**
     * Converts a Java value into a safe SQL literal.
     * Numeric values are left unquoted; strings are single-quoted with
     * internal single-quotes escaped.
     */
    static String sqlLiteral(Object value) {
        if (value == null) return "NULL";
        String text = String.valueOf(value);
        try {
            Double.parseDouble(text);
            return text;
        } catch (NumberFormatException ignored) {
            return "'" + text.replace("'", "''") + "'";
        }
    }

    /** Builds an aggregate-function expression string, e.g. {@code "AVG(salary)"}. */
    static String functionExpression(String funcName, String attrName) {
        return funcName.toUpperCase(Locale.ROOT) + "(" + attrName + ")";
    }
}
