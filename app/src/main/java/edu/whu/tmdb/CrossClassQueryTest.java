package edu.whu.tmdb;

import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.util.DbOperation;

/**
 * 跨类查询功能测试。
 * 每个测试独立展示一种语法能力。
 */
public class CrossClassQueryTest {

    public static void main(String[] args) {
        System.out.println("=== 跨类查询功能测试 ===\n");

        // Step 0: 清理环境
        exec("drop class exist;");
        System.out.println("[OK] 环境清理完成\n");

        // Step 1: 创建基础类
        // Song.description 故意不放入 singer_song，用于验证逆向导航
        exec("create class Singer(id int, name String, sex String, age String, nationality String, company String);");
        exec("create class Song(id int, name String, singer String, date int, description String);");
        System.out.println("[OK] 基础类创建完成: Singer, Song\n");

        // Step 2: 插入测试数据
        exec("insert into Singer values(0, 'TaylorSwift', 'F', 36, 'America', 'ATVMusic');");
        exec("insert into Singer values(1, 'EdSheeran', 'M', 33, 'UK', 'WarnerMusic');");
        exec("insert into Singer values(2, 'Adele', 'F', 35, 'UK', 'Columbia');");

        exec("insert into Song values(0, 'Red', 'TaylorSwift', 2012, 'Country pop album from 2012');");
        exec("insert into Song values(1, '1989', 'TaylorSwift', 2014, 'Synth-pop album from 2014');");
        exec("insert into Song values(2, 'Divide', 'EdSheeran', 2017, 'Pop album from 2017');");
        exec("insert into Song values(3, 'Hello', 'Adele', 2015, 'Soul ballad from 2015');");
        System.out.println("[OK] 测试数据插入完成\n");

        // Step 3: 验证基础数据
        System.out.println("--- 基础查询验证 ---");
        execPrint("select * from Singer;");
        execPrint("select * from Song;");

        // Step 4: 创建 deputy 类
        // singer_song: 连接 Song 和 Singer，但故意不包含 Song.description
        exec("create joindeputy singer_song as select Song.name, Song.date, Song.singer, Singer.sex, Singer.age, Singer.nationality from Song, Singer where Song.singer = Singer.name;");
        System.out.println("[OK] joindeputy 'singer_song' 创建完成\n");

        // 验证 deputy 类数据
        System.out.println("--- Deputy 类验证 ---");
        execPrint("select * from singer_song;");

        // Step 5: 跨类查询测试
        System.out.println("=== 跨类查询测试 ===\n");

        // 测试1: 单步导航 + 全属性
        System.out.println("--- 测试1: Singer -> singer_song (单步导航 + 全属性) ---");
        execPrint("SELECT Singer -> singer_song FROM Singer;");

        // 测试2: 单步导航 + 单属性
        System.out.println("--- 测试2: Singer -> singer_song.name (单步导航 + 单属性投影) ---");
        execPrint("SELECT Singer -> singer_song.name FROM Singer;");

        // 测试3: 起始类带筛选条件
        System.out.println("--- 测试3: Singer{name = 'TaylorSwift'} -> singer_song.name (起始类筛选) ---");
        execPrint("SELECT Singer{name = 'TaylorSwift'} -> singer_song.name FROM Singer;");

        // 测试4: 逆向导航 — description 不在 singer_song 中，必须跳回 Song
        System.out.println("--- 测试4: Singer -> singer_song -> Song.description (逆向导航) ---");
        execPrint("SELECT Singer -> singer_song -> Song.description FROM Singer;");

        // 测试5: 中间类筛选 — 过滤 singer_song 后，逆向取 Song.description
        System.out.println("--- 测试5: Singer -> singer_song{date >= 2014} -> Song.description (中间筛选) ---");
        execPrint("SELECT Singer -> singer_song{date >= 2014} -> Song.description FROM Singer;");

        // 测试6: AND 复合筛选 — 性别F且年龄>35，Adele(35)被年龄筛掉，EdSheeran(M)被性别筛掉，仅保留Taylor(36)
        System.out.println("--- 测试6: Singer{sex = 'F' AND age > 35} -> singer_song -> Song.description (AND 复合筛选) ---");
        execPrint("SELECT Singer{sex = 'F' AND age > 35} -> singer_song -> Song.description FROM Singer;");

        System.out.println("=== 跨类查询测试完成 ===");
    }

    private static void exec(String sql) {
        try {
            Main.execute(sql);
        } catch (Exception e) {
            System.out.println("SQL Error [" + sql + "]: " + e.getMessage());
        }
    }

    private static void execPrint(String sql) {
        try {
            SelectResult result = Main.execute(sql);
            if (result != null) {
                DbOperation.printResult(result);
            }
            System.out.println();
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
        }
    }
}
