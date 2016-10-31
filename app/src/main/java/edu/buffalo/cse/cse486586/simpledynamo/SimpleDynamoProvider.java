package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import static edu.buffalo.cse.cse486586.simpledynamo.Constants.*;
import static edu.buffalo.cse.cse486586.simpledynamo.ProviderHelper.*;


public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private final Uri myUri = ProviderHelper.buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	private static String[] cols = new String[] {GET_KEY, GET_VALUE};
	public static boolean flag_to_recover = false;
	public static boolean flag_to_create = false;
	public static boolean flag_to_insert = false;
	public static ConcurrentHashMap<String, String> getPortState = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, String> getQueries = new ConcurrentHashMap<String, String>();
	public static ConcurrentSkipListMap<String, String> hashedPorts = new ConcurrentSkipListMap<String, String>();
	public static ConcurrentHashMap<String, HashMap<String,String>> killed_avd_map = new ConcurrentHashMap<String, HashMap<String,String>>();
	public static ConcurrentHashMap<String, String> getMissedMsgMap = new ConcurrentHashMap<String, String>();


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		int returnValue = 0;
		Log.d(TAG, "Inside delete");
		try {
			if (selection.matches("\"*\""))
			{
				Log.d(TAG, "Selection is *");
				returnValue = writeToFile();
				if(selectionArgs == null)
				{
					//for(int i = 11108; i <= REMOTE_PORTS.size(); i+=4)
					for (String node_Counter : REMOTE_PORTS)
					{
						Log.d(TAG, "Port state:" + getPortState.get(node_Counter));
						if (!getPortState.get(node_Counter).equals(NODE_DEAD))
						{
							if (!node_Counter.equals(getPort()))
							{
								ClientTask client_task = new ClientTask();
								client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE_NODE + "%%" + selection + "%#%", node_Counter);
							}
						}
					}
				}
			}
			else if(selection.matches("\"@\""))
			{
				Log.d(TAG, "Selection is @");
				returnValue = writeToFile();
				Log.d(TAG, "Deleting all data from current avd" + returnValue);
			}
			else
			{
				writeToFile();
			}
			Log.e(TAG, "Deleting all data from current avd" + returnValue);
		}catch(Exception e)
		{
			Log.e(TAG, "Delete: Failed" + e.getMessage());
		}
		return returnValue;
	}

	private int writeToFile()
	{

		int returnValue = 0;
		try{
			File file = getContext().getFilesDir();
			File [] all_files = file.listFiles();

			for (File del_file : all_files)
			{
				getContext().deleteFile(del_file.getName());
				returnValue++;
			}

		} catch (Exception e) {
			Log.e("Delete: ", "Deleting files from avd failed");
		}
		return returnValue;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override

	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key_name = null;
		String key_hash = null;
		String content = null;

		String thisPort = getPort();
		try {
			for( String key: values.keySet() ) {
				if( key.equals( "key" ) ){
					key_name = String.valueOf( values.get( key ) );
					key_hash = genHash( key_name );
				} else {
					content = String.valueOf(values.get(key));
				}
			}
			Log.d(TAG, "Inserting key" + key_name );

			List<String> prefList = getPriority(findNeighbor(key_hash), true);

			if ( prefList.contains( thisPort ) ) {
				Date date = new Date();
				insertHelper(key_name, date.getTime() + "##" + content);
				prefList.remove( thisPort );
			}

			int myCount = 0;
			for( String pref : prefList ) {
				if(!chechDeadPort(pref)) {
					myCount ++;
				}
			}
			for (String port_name : prefList) {
				if (!chechDeadPort(port_name)) {
					Log.d(TAG, "Insert: inserting " + key_name);
					myCount--;
					//ClientTask client_task = new ClientTask();
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT_NODE + key_name + "%%" + content + "%%" + myCount, port_name);
				} else {
					Log.d(TAG, "Insert:missed at " + port_name);
					Date date = new Date();
					if (!killed_avd_map.containsKey(port_name)) {
						HashMap<String, String> tempMap = new HashMap();
						tempMap.put(key_name, date.getTime() + "##" + content);
						killed_avd_map.put(port_name, tempMap);
					} else {
						killed_avd_map.get(port_name).put(key_name, date.getTime() + "##" + content);
					}
				}
			}
			flag_to_insert = false;
			//looping
			while (!flag_to_insert){

			}
			flag_to_insert = false; // reset flag

		} catch ( Exception e ) {
			Log.e( TAG, "Insert: Failed");
			e.getMessage();
		}
		return uri;
	}

	@Override
	public boolean onCreate() {

		try {
			try {
				Log.d(TAG, "My ServerSocket");
				ServerSocket server_socket = new ServerSocket(SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, server_socket);
			} catch (IOException e) {
				Log.e(TAG, "onCreate: ServerSocket Failed to create");
				return false;
			}
			final String thisPort = getPort();

			Thread createThread = new Thread() {
				@Override
				public void run() {

					int iCount = 4;
					for (String port : REMOTE_PORTS) {
						try {
							hashedPorts.put(ProviderHelper.genHash(String.valueOf(Integer.valueOf(port) / 2)), port);

							getPortState.put(port, NODE_ALIVE);
							if (!port.equals(thisPort)) {
								iCount = iCount - 1;
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, NODE_ALIVE_STATUS + "%%" + thisPort + "%%" + String.valueOf(iCount), port);
							}
						} catch (Exception e) {
							Log.e(TAG, "OnCreate Failed due to exception" + e.getMessage());
						}
					}
					flag_to_create = false;
					while (!flag_to_create) {

					}
					flag_to_create = false;
					Log.d(TAG, "messages retrieved in MissedMap" + getMissedMsgMap.size());

					for (Map.Entry<String, String> entry : getMissedMsgMap.entrySet()) {
						insertHelper(entry.getKey(), entry.getValue());
					}
					getMissedMsgMap = new ConcurrentHashMap();
				}
			};
			createThread.start();
		}catch(Exception e){
			Log.e(TAG, "onCreate failed" + e.getMessage());
			e.printStackTrace();
		}
		return false;
	}

	@Override

	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		String read_line;
		MatrixCursor cursor = new MatrixCursor(cols);
		String value = null;
		FileInputStream fileInputStream;
		BufferedReader bufferedReader;
		Log.d(TAG, "Selection is"+ selection);

		try {
			if( selection.contains("*") ){
				if(selectionArgs == null) {
					synchronized ( this ) {
						getReplicas();
						List<String> aliveNodes = new ArrayList<String>();
						for (String port : getPortState.keySet()) {
							if (getPortState.get(port).equals(NODE_ALIVE)) {
								aliveNodes.add(port);
							}
						}
						aliveNodes.remove(getPort());

						int size = aliveNodes.size();
						for (String port : aliveNodes) {
							size = size - 1;
							//ClientTask clientTask = new ClientTask();
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, GLOBAL_STAR_QUERY + "%%" + selection + "%%" + String.valueOf(size), port);
						}
						flag_to_recover = false;
						while (!flag_to_recover) {

						}
						Log.d(TAG, "GQuery over recover now");
						flag_to_recover = false;
						for (String key : getQueries.keySet()) {
							String values[] = getQueries.get(key).split("##");
							values[0] = key;
							cursor.addRow(values);
						}
						getQueries = new ConcurrentHashMap();
					}
				} else {
					Log.d(TAG, "Sending back ack ");
					for (String file : getContext().fileList()) {
						fileInputStream = getContext().openFileInput(file);
						bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

						while ((read_line = bufferedReader.readLine()) != null) {
							value = read_line;
						}
						String[] values = new String[]{file, value};
						cursor.addRow(values);
					}
				}
				value = null;
				Log.d( TAG, "Files retrieved: " + cursor.getCount() );
				return cursor;
			} else if( selection.matches( "@" ) ) {

				Log.d(TAG, "Sending back ack ");
				for (String file : getContext().fileList()) {
					fileInputStream = getContext().openFileInput(file);
					bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

					while ((read_line = bufferedReader.readLine()) != null) {
						String[] tempSplit = read_line.split("##");
						value = tempSplit[1];
					}
					String[] values = new String[]{file, value};
					cursor.addRow(values);
				}
				value = null;
				Log.d( TAG, "Files retrieved: " + cursor.getCount());
				return cursor;
			} else {
				if(selectionArgs == null) {
					synchronized ( this ) {
						List<String> prefList = getPriority(findNeighbor(ProviderHelper.genHash(selection)), false );
						int prefSize = prefList.size();
						for (String port : prefList) {
							prefSize = prefSize - 1;
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, LOCAL_STAR_QUERY + "%%" + selection + "%%" + String.valueOf(prefSize), port);
						}
						//create recovery_flag
						flag_to_recover = false;
						while (!flag_to_recover) {

						}
						Log.d(TAG, "LQuery over recover now");
						flag_to_recover = false;
						if (getQueries.get(selection) != null) {
							String tempSplit[] = getQueries.get(selection).split("##");
							value = tempSplit[1];
						}
						getQueries = new ConcurrentHashMap();
						if( value != null ) {
							String[] values = new String[]{selection, value};
							cursor.addRow(values);
						}
					}
				} else {
					Log.d(TAG, "Sending back ack ");
					try {
						fileInputStream = getContext().openFileInput(selection);
						bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
						while ((read_line = bufferedReader.readLine()) != null) {
							value = read_line;
						}
					} catch (Exception e) {
						Log.e(TAG, "Unable to open file in Query");
					}
					if( value != null ) {
						String[] values = new String[]{selection, value};
						cursor.addRow(values);
					}
				}
				value = null;
				return cursor;
			}
		} catch (Exception e) {
			Log.e(TAG, "Query failed due to : " + e.getMessage());
			e.printStackTrace();
		}
		return cursor;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			HashMap<String,String> serverMap = new HashMap();
			serverMap.put(GET_KEY, GET_VALUE);
			String msgToRecv = null;
			Socket server_socket;
			String msgTosend;

			try {
				while(true)
				{
					server_socket = serverSocket.accept();

					ObjectInputStream objectInputStream = new ObjectInputStream(server_socket.getInputStream());
					if ((msgToRecv = (String)objectInputStream.readObject())!= null)
					{
						msgToRecv = msgToRecv.trim();

						ObjectOutputStream outputStream = new ObjectOutputStream( server_socket.getOutputStream() );

						if( msgToRecv.contains(NODE_ALIVE_STATUS))
						{
							Log.d(TAG, "Msg received : Alive");
							String splitString[] = msgToRecv.split("%%");
							if(getPortState.containsKey( splitString[1]))
							{
								getPortState.put(splitString[1], NODE_ALIVE);
							}
							outputStream.writeObject(killed_avd_map.get(splitString[1]));
							killed_avd_map.remove(splitString[1]);
							outputStream.close();

						}

						else if( msgToRecv.contains(NODE_DEAD_STATUS))
						{
							Log.d(TAG, "Msg received : Dead");
							String splitString[] = msgToRecv.split("%%");

							if(getPortState.containsKey( splitString[1]))
							{
								getPortState.put(splitString[1], NODE_DEAD);
							}
							outputStream.writeObject(serverMap);

						}

						else if(msgToRecv.contains(DELETE_NODE))
						{
							Log.d(TAG, "Msg received : Delete");
							String strReceiveSplit[] = msgToRecv.split("%%");
							getContext().getContentResolver().delete(myUri, strReceiveSplit[1], strReceiveSplit);
							outputStream.writeObject(serverMap);

						}


						else if(msgToRecv.contains(GLOBAL_STAR_QUERY))
						{
							Log.d(TAG, "Msg received : Query Global");
							String recvArgs[] = msgToRecv.split("%%");
							String sendArgs[] = new String[]{recvArgs[1]};
							Cursor cursor = getContext().getContentResolver().query(myUri, null, recvArgs[1], sendArgs, null);

							HashMap<String, String> valueToReturn = new HashMap<String, String>();

							try {

								if (cursor != null)
								{

									int keyIndex = cursor.getColumnIndex(GET_KEY);
									int valueIndex = cursor.getColumnIndex(GET_VALUE);

									for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext())
									{
										//check in next two successors
										valueToReturn.put(cursor.getString(keyIndex), cursor.getString(valueIndex));
									}
									cursor.close();
								}
							}catch( Exception e )
							{
								e.printStackTrace();

							}
							outputStream.writeObject(valueToReturn);
						}

						else if(msgToRecv.contains(LOCAL_STAR_QUERY))
						{
							Log.d(TAG, "Msg received : Local Query");

							String strReceiveSplit[] = msgToRecv.split("%%");
							String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
							HashMap<String, String> returnValue = new HashMap<String, String>();

							try
							{
								Cursor resultCursor = getContext().getContentResolver().query(myUri, null, strReceiveSplit[1], sendSelectArgs, null);
								if (resultCursor != null)
								{
									int valueIndex = resultCursor.getColumnIndex(GET_VALUE);
									if (valueIndex != -1)
									{
										resultCursor.moveToFirst();
										returnValue.put(strReceiveSplit[1], resultCursor.getString(valueIndex));
									}
									resultCursor.close();
								}
							}
							catch( Exception e )
							{
								Log.e(TAG, "Exception in ServerTask"+ e.getMessage());

							}
							outputStream.writeObject(returnValue);
						}


						else if(msgToRecv.contains(INSERT_NODE))
						{
							Log.d(TAG, "Msg received : Insert");
							msgToRecv = msgToRecv.replace(INSERT_NODE,"");

							StringTokenizer stringTokenizer = new StringTokenizer(msgToRecv,"%%");

							Date date = new Date();

							insertHelper(stringTokenizer.nextToken(), date.getTime() + "##" + stringTokenizer.nextToken());

							outputStream.writeObject(serverMap);
						}

						outputStream.close();
					}
					objectInputStream.close();
				}
			}catch(Exception e){
				Log.e(TAG, "ServerTask Exception"+ e.getMessage());
			}
			return null;
		}

		protected void onProgressUpdate(String...strings) {

		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	public static String findNeighbor(String input)
	{

		String neighbor = null;
		try {
			for (String key : hashedPorts.keySet())
			{
				if (input.compareTo(key) < 0)
				{
					neighbor = hashedPorts.get(key);
					break;
				}
				if (neighbor == null)
				{
					neighbor = hashedPorts.firstEntry().getValue();
				}
			}
		} catch (Exception e) {
			Log.e(TAG, "FindNeighbor: failed = " + e.getMessage());
			e.printStackTrace();
		}
		return neighbor;
	}

	private String getPort() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return String.valueOf((Integer.parseInt(portStr) * 2));
	}


	public void insertHelper(String get_file, String content)
	{

		try{
			FileOutputStream output = getContext().openFileOutput( get_file, Context.MODE_PRIVATE);
			output.write(content.getBytes());
			output.close();
		}catch(Exception e){
			Log.e(TAG, "insertHelper: Failed" + e.getMessage());
			e.printStackTrace();
		}
	}

	public static List<String> getPriority(String input, boolean checkDead) {

		List<String> returnValue = new ArrayList<String>();
		try {
			Log.d(TAG, "Inside getPriority");
			input = ProviderHelper.genHash(String.valueOf(Integer.valueOf(input) / 2));
			int count = 3;

			for (Map.Entry<String, String> entry : hashedPorts.entrySet()) {
				if (input.compareTo(entry.getKey()) <= 0)
				{
					if (checkDead || !chechDeadPort(entry.getValue()))
					{
						returnValue.add(entry.getValue());
					}
					count--;
					if (count == 0)
						break;
				}
			}

			if (count != 0) {
				for (Map.Entry<String, String> entry : hashedPorts.entrySet())
				{
					if (checkDead || !chechDeadPort(entry.getValue()))
					{
						Log.d(TAG, "Check Condition in Priority");
						returnValue.add(entry.getValue());
					}
					count--;
					if (count == 0)
						break;

				}
			}

		} catch (Exception e) {
			Log.e(TAG, "Error in getPriority" + e.getMessage());
		}
		return returnValue;
	}

	private void getReplicas()
	{

		String read_line;
		BufferedReader bufferedReader;
		try{
			for (String get_file : getContext().fileList())
			{
				FileInputStream input = getContext().openFileInput(get_file);
				bufferedReader = new BufferedReader(new InputStreamReader(input));

				while ((read_line = bufferedReader.readLine()) != null) {
					getQueries.put(get_file, read_line);
				}
			}
		}catch(Exception e){
			Log.e(TAG, "getReplicas: Failed");
			e.printStackTrace();
		}
	}

	/*class Waiter extends Thread {
		Waiter() {
			this.start();
		}

		public void run() {
			while(true) {

				boolean receivedReponse = false;
				int indexGlobalData = 0;

				if(receivedReponse ||
						indexGlobalData >= 4) {
					Log.d(TAG, "ReceivedNode got set to " + receivedReponse);
					break;
				}
			}
		}
	}*/

	public static boolean chechDeadPort(String input)
	{

		return getPortState.get(input).equals(NODE_DEAD);
	}
}