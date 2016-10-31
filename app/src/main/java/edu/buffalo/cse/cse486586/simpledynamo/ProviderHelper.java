package edu.buffalo.cse.cse486586.simpledynamo;

import android.net.Uri;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 * Created by debika on 4/24/16.
 */
public class ProviderHelper {

    private static final String TAG = ProviderHelper.class.getSimpleName();

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    /*private void getNode(String nodeID, String predecessor, String successor){


        try {

            node.setSuccessor(successor_node);
            node.setMin_node(nodeID);
            node.setMax_node(nodeID);

            node.setPortID(nodeID);
            node.setHashVal(genHashPort(nodeID));

            predecessor_node.setPortID(nodeID);
            predecessor_node.setHashVal(genHashPort(nodeID));

            node.setPredecessor(predecessor_node);

            successor_node.setPortID(nodeID);
            successor_node.setHashVal(genHashPort(nodeID));

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }*/

}