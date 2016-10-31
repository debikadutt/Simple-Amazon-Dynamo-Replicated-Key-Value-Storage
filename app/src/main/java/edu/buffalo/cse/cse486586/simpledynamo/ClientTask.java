package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import static edu.buffalo.cse.cse486586.simpledynamo.Constants.*;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.*;

/**
 * Created by debika on 4/24/16.
 */


public class ClientTask extends AsyncTask<String, Void, Void> {

    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    @Override
    protected Void doInBackground( String... msgs ) {

        ObjectOutputStream writer;
        ObjectInputStream reader;
        Socket server_socket;
        String msgToSend = msgs[0];
        String splitMsg[] = null;
        try {
            splitMsg = msgToSend.split( "%%" );
            server_socket = new Socket( InetAddress.getByAddress( new byte[]{10, 0, 2, 2} ),
                    Integer.parseInt( msgs[1] ) );
            server_socket.setSoTimeout( 3000 );

            writer = new ObjectOutputStream( server_socket.getOutputStream() );
            writer.writeObject( new String( msgToSend ) );

            reader = new ObjectInputStream(server_socket.getInputStream());
            HashMap<String, String> map = (HashMap) reader.readObject();
            if (map != null) {
                if ( msgToSend.startsWith( NODE_QUERY ) ) {
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        if (getQueries.containsKey(entry.getKey())) {
                            String splitStrNew[] = entry.getValue().split("##");
                            String splitStrOld[] = getQueries.get(entry.getKey()).split("##");
                            if (Long.valueOf(splitStrNew[0]) > Long.valueOf(splitStrOld[0])) {
                                getQueries.put(entry.getKey(), entry.getValue());
                            }
                        } else {
                            getQueries.put(entry.getKey(), entry.getValue());
                        }
                    }

                } else if ( msgToSend.startsWith( NODE_ALIVE_STATUS ) ) {
                    Log.e(TAG, "avd messages missed: " + map.size());
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        getMissedMsgMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            writer.close();
            reader.close();
            server_socket.close();
        } catch ( IOException ioe ) {
            Log.e( TAG, "Inside ClientTask IOException");
            if(!msgs[0].contains(NODE_ALIVE_STATUS)) {
                recoverMessages(msgs[0], msgs[1]);
                handleException(msgs[1]);
            }
        } catch ( Exception e) {
            Log.e( TAG, "ClientTask failed due to" + e.getMessage() );
        } finally{
            if (Integer.valueOf(splitMsg[2]) == 0) {
                Log.e(TAG, "Recovery flag set true");
                if(msgToSend.startsWith( NODE_QUERY )) {
                    SimpleDynamoProvider.flag_to_recover = true;
                } else if(msgToSend.startsWith( NODE_ALIVE_STATUS )) {
                    flag_to_create = true;
                } else if(msgToSend.startsWith( INSERT_NODE )) {
                    flag_to_insert = true;
                }
            }
        }
        return null;
    }

    public void handleException(String port){

        try {
            Log.e( TAG, "Port dead is" + port );
            getPortState.put( port, NODE_DEAD );
            for (String strPort : getPortState.keySet()) {
                if ( !chechDeadPort( strPort ) && !strPort.equals( port ) ) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(strPort));
                        socket.setSoTimeout( 3000 );
                        ObjectOutputStream writer = new ObjectOutputStream( socket.getOutputStream() );
                        writer.writeObject( NODE_DEAD_STATUS + "%%" + port );

                        ObjectInputStream reader = new ObjectInputStream(socket.getInputStream());
                        HashMap<String, String> map = (HashMap) reader.readObject();
                        writer.close();reader.close();
                        socket.close();
                    }catch (IOException e) {
                        Log.e(TAG, "IOException in ClientTask");
                    }catch (Exception e) {}
                }
            }
        } catch ( Exception e ) {
            Log.e(TAG, "Exception in ClientTask");
        }
    }

    public void recoverMessages(String msg1, String msg2){
        try {
            if ( msg1.contains( INSERT_NODE ) ) {
                String splitTemp[] = msg1.replace( INSERT_NODE, "" ).split( "%%" );
                Log.d( TAG, "recovery failed: " + msg2);
                Date date = new Date();
                if ( !killed_avd_map.containsKey( msg2 ) ) {
                    HashMap<String, String> tempMap = new HashMap();
                    tempMap.put( splitTemp[0], String.valueOf( date.getTime() ) + "##" + splitTemp[1] );
                    killed_avd_map.put(msg2, tempMap);
                } else {
                    killed_avd_map.get( msg2 ).put( splitTemp[0], String.valueOf(date.getTime()) + "##" + splitTemp[1] );
                }

            }
        }catch( Exception e ) {
            Log.e( TAG, "recoverMessages failed due to" + e.getMessage() );
        }
    }
}