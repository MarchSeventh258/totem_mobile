package edu.whu.tmdb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.impl.CrossClassQueryImpl;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.util.DbOperation;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.CrossClassPathExpression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Main {
    public static String execute_UI_single(String sqlCommand){
        dropExistClassSafely();
        // 调试用
        // System.out.print("tmdb> ");
        if ("resetdb".equalsIgnoreCase(sqlCommand) || "rst".equalsIgnoreCase(sqlCommand)) {
            return DbOperation.getResetDB();
        } else if ("show BiPointerTable".equalsIgnoreCase(sqlCommand)||"showb".equalsIgnoreCase(sqlCommand)) {
            return DbOperation.getBiPointerTableString();
        } else if ("show ClassTable".equalsIgnoreCase(sqlCommand)||"showc".equalsIgnoreCase(sqlCommand)) {
            return DbOperation.getClassTableString();
        } else if ("show AttributeTable".equalsIgnoreCase(sqlCommand)||"showa".equalsIgnoreCase(sqlCommand)) {
            return DbOperation.getArributeTableString();
        }else if ("show DeputyTable".equalsIgnoreCase(sqlCommand)||"showd".equalsIgnoreCase(sqlCommand)) {
            return DbOperation.getDeputyTableString();
        } else if ("show SwitchingTable".equalsIgnoreCase(sqlCommand)||"shows".equalsIgnoreCase(sqlCommand)) {
            return DbOperation.getSwitchingTableString();
        } else if (!sqlCommand.isEmpty()) {
            try {
                SelectResult result = execute(sqlCommand);
                if (result != null) {
                    return DbOperation.getResultString(result);
                }
                else return "success";
            } catch (Exception e) {
                e.printStackTrace();
                return "Error: " + shortMessage(e.getMessage());  // 返回错误信息
            }
        }
        return "";
    }

    public static String[] execute_UI(String sqlCommands) {
        List<String> results = new ArrayList<>();
        // 按分号切分（忽略单引号字符串内的分号），语句内部允许换行；
        // 无法整体解析且含换行的段按行拆分，兼容不带分号、每行一条命令的输入
        for (String piece : sqlCommands.split(";(?=(?:[^']*'[^']*')*[^']*$)")) {
            String cmd = piece.trim();
            // 只有当一段里所有非空行都是 -- 注释时才整段跳过；
            // 若注释行后面还跟着真实语句，则交给解析器处理（语句内的注释会被忽略）
            if (cmd.isEmpty()) {
                continue;
            }
            boolean allComments = true;
            for (String line : cmd.split("\n")) {
                String l = line.trim();
                if (!l.isEmpty() && !l.startsWith("--")) {
                    allComments = false;
                    break;
                }
            }
            if (allComments) {
                continue;
            }
            if (!canParse(cmd) && cmd.contains("\n")) {
                // 段内可能混有"整句 + 注释 + 跨行语句"：逐行累积，
                // 累积到能完整解析就作为一条命令执行
                StringBuilder buf = new StringBuilder();
                for (String line : cmd.split("\n")) {
                    String l = line.trim();
                    if (l.isEmpty() || l.startsWith("--")) {
                        continue;
                    }
                    String trial = buf.length() == 0 ? l : buf + " " + l;
                    if (canParse(trial)) {
                        results.add(runOne(trial));
                        buf.setLength(0);
                    } else {
                        buf.append(' ').append(l);
                    }
                }
                if (buf.length() > 0) {
                    results.add(runOne(buf.toString().trim()));
                }
            } else {
                results.add(runOne(cmd));
            }
        }
        // 转换为数组返回
        return results.toArray(new String[0]);
    }

    /** 判断一条 SQL 能否被完整解析，用于区分"整段是一个语句"与"多条命令堆叠"。 */
    private static boolean canParse(String sql) {
        try {
            CCJSqlParserUtil.parse(sql);
            return true;
        } catch (Exception | TokenMgrError e) {
            return false;
        }
    }

    /** 执行单条命令，任何错误（含 Error）都转为文本返回，避免 UI 闪退。 */
    private static String runOne(String sql) {
        try {
            String singleResult = execute_UI_single(sql);
            return singleResult != null ? singleResult : "";
        } catch (Throwable e) {
            e.printStackTrace();
            return "Error: " + shortMessage(e.getMessage());
        }
    }

    /** 把异常信息压缩成一行并去掉类名前缀，避免 UI 输出 JSqlParser 的冗长错误详情。 */
    private static String shortMessage(String msg) {
        if (msg == null) return "unknown error";
        if (msg.contains("\n")) msg = msg.substring(0, msg.indexOf("\n"));
        int idx = msg.indexOf(": ");
        return (idx >= 0) ? msg.substring(idx + 2) : msg;
    }

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String sqlCommand;

        // 调试用
        dropExistClassSafely();
        while (true) {
            System.out.print("tmdb> ");
            sqlCommand = reader.readLine().trim();
            if ("exit".equalsIgnoreCase(sqlCommand)) {
                break;
            } else if (sqlCommand.startsWith("--")) {
                continue;   // 注释行，跳过
            } else if ("resetdb".equalsIgnoreCase(sqlCommand) || "rst".equalsIgnoreCase(sqlCommand)) {
                DbOperation.resetDB();
            } else if ("show AttributeTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showAttributeTable();
            } else if ("show BiPointerTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showBiPointerTable();
            } else if ("show ClassTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showClassTable();
            } else if ("show DeputyTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showDeputyTable();
            } else if ("show SwitchingTable".equalsIgnoreCase(sqlCommand)) {
                DbOperation.showSwitchingTable();
            } else if (!sqlCommand.isEmpty()) {
                SelectResult result = execute(sqlCommand);
                if (result != null) {
                    try {
                        DbOperation.printResult(result);
                    } catch (TMDBException e) {
                        // 处理异常，例如记录日志、提示用户等
                        System.out.println("An error occurred: " + e.getMessage());
                    }
                }
            }
        }

        // execute("show tables;");
        // execute(args[0]);
        // transaction.test();
        // transaction.test2();
        // insertIntoTrajTable();
        // testMapMatching();
        // testEngine();
        // testTorch3();
    }

    /** Best-effort cleanup of a leftover class named "exist" from a previous
     *  session. The class usually does not exist, so failure is ignored. */
    private static void dropExistClassSafely() {
        try {
            execute("drop class exist;");
        } catch (RuntimeException ignored) {
            // no such class — nothing to clean up
        }
    }

    public static SelectResult execute(String s)  {
        Transaction transaction = Transaction.getInstance();    // 创建一个事务实例
        SelectResult selectResult = null;
        try {
            // 使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
            Statement stmt = CCJSqlParserUtil.parse(byteArrayInputStream);

            // 检测跨类查询：如果解析后的Select包含CrossClassPathExpression
            if (stmt instanceof net.sf.jsqlparser.statement.select.Select) {
                net.sf.jsqlparser.statement.select.Select selectStmt =
                        (net.sf.jsqlparser.statement.select.Select) stmt;
                if (selectStmt.getSelectBody() instanceof PlainSelect) {
                    PlainSelect plainSelect = (PlainSelect) selectStmt.getSelectBody();
                    for (SelectItem item : plainSelect.getSelectItems()) {
                        if (item instanceof CrossClassPathExpression) {
                            CrossClassQueryImpl crossClassQuery = new CrossClassQueryImpl();
                            return crossClassQuery.execute(plainSelect,
                                    (CrossClassPathExpression) item);
                        }
                    }
                }
            }

            selectResult = transaction.query("", -1, stmt);
            if(!stmt.getClass().getSimpleName().toLowerCase().equals("select")){
                transaction.SaveAll();
            }
        }catch (JSQLParserException e) {
            System.out.println("syntax error");
        } catch (TokenMgrError e) {
            // TokenMgrError extends Error, not Exception — must be caught here,
            // otherwise lexical errors (e.g. a full-width ';') crash the app
            System.out.println("syntax error");
            throw new RuntimeException(e);
        } catch (TMDBException e) {
            System.out.println("Cross-class query error: " + e.getMessage());
        }
        return selectResult;
    }

}