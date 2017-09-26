package study.streaming.upgrade;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;

public class JavaCustomReceiver extends Receiver<String> {

	private static final long serialVersionUID = 705941735785082332L;
	
	String host = null;
	int port = -1;

	public JavaCustomReceiver(String host_ , int port_) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		host = host_;
		port = port_;
	}
  
	public void onStart() {
		new Thread()  {
			@Override 
			public void run() {
				receive();
			}
		}.start();
	}

	public void onStop() {

	}

	private void receive() {
		Socket socket = null;
		String userInput = null;

		try {
			socket = new Socket(host, port);

			BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			
			while (!isStopped() && (userInput = reader.readLine()) != null) {
				System.out.println("Received data '" + userInput + "'");
				store(userInput);
			}
			reader.close();
			socket.close();
			
			restart("Trying to connect again");
		} catch(ConnectException ce) {
			restart("Could not connect", ce);
		} catch(Throwable t) {
			restart("Error receiving data", t);
		}
	}
	
}