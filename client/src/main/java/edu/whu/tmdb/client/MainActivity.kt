package edu.whu.tmdb.client

import android.content.ComponentName
import android.content.Intent
import android.content.ServiceConnection
import android.os.Bundle
import android.os.IBinder
import android.os.RemoteException
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import edu.whu.tmdb.ISqlExecService

class MainActivity : AppCompatActivity() {

    private val targetPackage = "edu.whu.tmdb"
    private val targetAction = "edu.whu.tmdb.action.SQL_EXEC_SERVICE"

    private lateinit var etSql: EditText
    private lateinit var tvStatus: TextView
    private lateinit var tvResult: TextView
    private var remoteService: ISqlExecService? = null
    private var bound = false

    private val connection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName, service: IBinder) {
            remoteService = ISqlExecService.Stub.asInterface(service)
            bound = true
            tvStatus.text = "Connected: ${name.flattenToShortString()}"
        }

        override fun onServiceDisconnected(name: ComponentName) {
            remoteService = null
            bound = false
            tvStatus.text = "Disconnected"
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        etSql = findViewById(R.id.etSql)
        tvStatus = findViewById(R.id.tvStatus)
        tvResult = findViewById(R.id.tvResult)
        val btnConnect: Button = findViewById(R.id.btnConnect)
        val btnExecute: Button = findViewById(R.id.btnExecute)

        etSql.setText("show ClassTable")

        btnConnect.setOnClickListener { connectService() }
        btnExecute.setOnClickListener { executeSql() }
    }

    private fun connectService() {
        if (bound) {
            tvStatus.text = "Already connected"
            return
        }

        val intent = Intent(targetAction).apply {
            setPackage(targetPackage)
        }
        val ok = bindService(intent, connection, BIND_AUTO_CREATE)
        tvStatus.text = if (ok) "Binding..." else "Bind failed"
    }

    private fun executeSql() {
        val service = remoteService
        if (service == null) {
            tvResult.text = "Service not connected. Tap Connect first."
            return
        }

        val sql = etSql.text?.toString()?.trim().orEmpty()
        if (sql.isEmpty()) {
            tvResult.text = "Please enter SQL."
            return
        }

        try {
            val result = service.executeSql(sql)
            tvResult.text = result.orEmpty()
        } catch (e: RemoteException) {
            tvResult.text = "RemoteException: ${e.message}"
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        if (bound) {
            unbindService(connection)
            bound = false
        }
        remoteService = null
    }
}
