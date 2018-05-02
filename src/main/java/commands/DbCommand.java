package commands;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import commands.Command;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

public class DbCommand extends Command {

    public void execute() {
        HashMap<String, Object> props = parameters;

        Channel channel = (Channel) props.get("channel");
        System.out.println( "Received: " + props.get("body"));
        AMQP.BasicProperties properties = (AMQP.BasicProperties) props.get("properties");
        AMQP.BasicProperties replyProps = (AMQP.BasicProperties) props.get("replyProps");
        java.sql.Connection conn = (java.sql.Connection) props.get("dbConnection");
        Envelope envelope = (Envelope) props.get("envelope");
        String callStatement = "";
        int outType;
        JSONArray inputArray;
        try {
            JSONParser parser = new JSONParser();
            JSONObject body = (JSONObject) parser.parse((String) props.get("body"));


            callStatement = body.get("call_statement").toString();
            outType =Integer.parseInt( body.get("out_type").toString());
            inputArray = (JSONArray) body.get("input_array");
            String response = callDatabase(callStatement,outType,inputArray, conn);
            System.out.println("Sent "+response);
            channel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    public String callDatabase(String callStatement, int outType, JSONArray inputArray, java.sql.Connection conn){

        JSONArray jsonArray = new JSONArray();

        try {
            conn.setAutoCommit(false);
            CallableStatement upperProc = conn.prepareCall(callStatement);
            int i = 1;
            if(outType != 0){
            upperProc.registerOutParameter(1,outType);
            i++;
            }

            for(int j=0;j<inputArray.size();j++){
                JSONObject o = (JSONObject) inputArray.get(j);
                int type = Integer.parseInt(o.get("type").toString());
                switch (type){
                    case Types.INTEGER :{
                        int value =  Integer.parseInt(o.get("value").toString());
                        upperProc.setInt(i,value);
                        break;
                    }
                    case Types.VARCHAR:{
                        String value = o.get("value").toString();
                        upperProc.setString(i,value);
                        break;
                    }

                }
                i++;
            }
            upperProc.execute();

            conn.commit();

            switch (outType){
                case Types.OTHER:{
                ResultSet rs = (ResultSet) upperProc.getObject(1);

                ResultSetMetaData resultSetMetaData = rs.getMetaData();
                while (rs.next()) {
                    JSONObject json = new JSONObject();
                    for(int j=1;j<resultSetMetaData.getColumnCount()+1;j++){
                    switch (resultSetMetaData.getColumnType(j)){
                        case Types.VARCHAR :
                            json.put(resultSetMetaData.getColumnName(j),rs.getString(j));
                            break;
                    }
                    }
                    jsonArray.add(json);
                }
                    rs.close();
                break;
                }
                case Types.INTEGER: {
                    JSONObject json = new JSONObject();
                    json.put("Updated Rows",upperProc.getInt(1));
                    jsonArray.add(json);
                    break;
                }
            }


         }catch (SQLException e) {
            if(Integer.parseInt(e.getSQLState()) == 23505){
                return  "This email already exists.";
            }
            e.printStackTrace();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if (conn != null) {
                try { conn.close(); } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return jsonArray.toString();
    }
}
