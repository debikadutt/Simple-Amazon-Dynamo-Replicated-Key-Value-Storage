package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by debika on 4/24/16.
 */

public class Constants {

    public static final int SERVER_PORT = 10000;
    public static final String GET_KEY = "key";
    public static final String GET_VALUE = "value";
    public static final String DELETE_NODE = "delete";
    public static final String GLOBAL_STAR_QUERY = "queryGlobal";
    public static final String LOCAL_STAR_QUERY = "queryLocal";
    public static final String NODE_QUERY = "query";
    public static final String INSERT_NODE = "insert";
    public static final String NODE_ALIVE = "alive";
    public static final String NODE_DEAD = "dead";
    public static final String NODE_DEAD_STATUS ="reportDead";
    public static final String NODE_ALIVE_STATUS ="reportAlive";

    public static List<String> REMOTE_PORTS = new ArrayList<String>()
    {{
            add("11108");
            add("11112");
            add("11116");
            add("11120");
            add("11124");
    }};
}

