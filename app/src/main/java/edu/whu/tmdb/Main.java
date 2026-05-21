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
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.CrossClassPathExpression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Main {
    public static String execute_UI_single(String sqlCommand){
        @SuppressWarnings("unused")
        SelectResult resultt = execute("drop class exist;");
        // 调试用
        // System.out.print("tmdb> ");
        if ("resetdb".equalsIgnoreCase(sqlCommand)) {
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
                return "Error: " + e.getMessage();  // 返回错误信息
            }
        }
        return "";
    }

    public static String[] execute_UI(String sqlCommands) {
        // 按换行符分割多条指令
        String[] commands = sqlCommands.split("\n");
        List<String> results = new ArrayList<>();

        for (String command : commands) {
            String trimmedCommand = command.trim();

            // 跳过空指令
            if (trimmedCommand.isEmpty()) {
                continue;
            }

            try {
                // 调用原有的单条指令执行函数
                String singleResult = execute_UI_single(trimmedCommand);
                results.add(singleResult != null ? singleResult : "");

            } catch (Exception e) {
                results.add("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // 转换为数组返回
        return results.toArray(new String[0]);
    }

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String sqlCommand;

        @SuppressWarnings("unused")
        SelectResult resultt = execute("drop class exist;");
        // 调试用
        while (true) {
            System.out.print("tmdb> ");
            sqlCommand = reader.readLine().trim();
            if ("exit".equalsIgnoreCase(sqlCommand)) {
                break;
            } else if ("resetdb".equalsIgnoreCase(sqlCommand)) {
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
        } catch (TMDBException e) {
            System.out.println("Cross-class query error: " + e.getMessage());
        }
        return selectResult;
    }

}