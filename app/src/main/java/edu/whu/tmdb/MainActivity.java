package edu.whu.tmdb;

import android.content.Intent;
import android.graphics.Typeface;
import android.os.Bundle;
import android.text.Html;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.HorizontalScrollView;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;
import androidx.appcompat.app.AppCompatActivity;

import edu.whu.tmdb.R;
import edu.whu.tmdb.Main;

public class MainActivity extends AppCompatActivity {

    private EditText etCmd;
    private LinearLayout resultContainer;
    private ScrollView verticalScrollView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        // 初始化视图
        etCmd = findViewById(R.id.etCmd);
        resultContainer = findViewById(R.id.resultContainer);
        verticalScrollView = findViewById(R.id.scrollView);

        Button btExecute = findViewById(R.id.btExecute);
        Button btClear = findViewById(R.id.btClear);

        // 执行按钮逻辑
        btExecute.setOnClickListener(v -> {
            String sqlCommand = etCmd.getText().toString().trim();
            if (sqlCommand.isEmpty()) {
                Toast.makeText(this, "请输入SQL命令", Toast.LENGTH_SHORT).show();
                return;
            }

            // 生成原始结果
            String rawResult = Main.execute_UI_single(sqlCommand);

            // 创建横向滚动容器
            HorizontalScrollView hsv = new HorizontalScrollView(this);
            hsv.setLayoutParams(new LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT));

            // 创建文本视图
            TextView tv = new TextView(this);
            tv.setLayoutParams(new LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT));

            // 关键修改：设置等宽字体和HTML格式
            String htmlResult = "<pre><tt>" +
                    rawResult.replace(" ", "&nbsp;")
                            .replace("\n", "<br>") +
                    "</tt></pre>";
            tv.setText(Html.fromHtml(htmlResult, Html.FROM_HTML_MODE_LEGACY));

            tv.setTextSize(16);
            tv.setTypeface(Typeface.MONOSPACE); // 强制等宽字体
            tv.setTextColor(getResources().getColor(android.R.color.black));

            hsv.addView(tv);
            resultContainer.addView(hsv);

            // 自动滚动到底部
            verticalScrollView.post(() -> verticalScrollView.fullScroll(View.FOCUS_DOWN));
        });

        // 清屏按钮逻辑
        btClear.setOnClickListener(v -> resultContainer.removeAllViews());

    }
}