package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private static int SERVER_PORT = 10000;
	private List<String> EMULATORS = new ArrayList<String>();
	private static String myId = null;
	private static String predecessorPort = null;
	private static String successorPort = null;
	private static String myPort = null;
	private static String portStr = null;
	private static String firstHash = null;
	TreeMap<String,String> HashedPortsMap = new TreeMap<String, String>();
	private static boolean wait = false;

	private List<String> files = new LinkedList<String>();
	List<String> listOfGenHash = new ArrayList<String>();

	List<StoreMessagesOnFailure> failureMessages = new LinkedList<StoreMessagesOnFailure>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		synchronized (this) {
			Context context = getContext();

			try {
				String id = genHash(selection);

				if (listOfGenHash != null) {
					for (int i = 0; i < listOfGenHash.size(); i++) {
						TreeMap<Integer, Integer> IndicesMap = new TreeMap<Integer, Integer>();

						if (listOfGenHash.get(i).equals(firstHash)) {
							if (lookUp(id, listOfGenHash.get(i), listOfGenHash.get(4))) {
								IndicesMap.put(i, i + 1);
								IndicesMap.put(i + 1, i + 2);
								IndicesMap.put(i + 2, i + 1);
							}
						} else {
							if (lookUp(id, listOfGenHash.get(i), listOfGenHash.get(i - 1))) {
								if (i == listOfGenHash.size() - 1) {
									IndicesMap.put(i, 0);
									IndicesMap.put(0, 1);
									IndicesMap.put(1, 0);

								} else if (i == listOfGenHash.size() - 2) {
									IndicesMap.put(i, i + 1);
									IndicesMap.put(i + 1, 0);
									IndicesMap.put(0, i + 1);
								} else {
									IndicesMap.put(i, i + 1);
									IndicesMap.put(i + 1, i + 2);
									IndicesMap.put(i + 2, i + 1);
								}
							}
						}

						if (IndicesMap != null) {
							for (TreeMap.Entry<Integer, Integer> entry : IndicesMap.entrySet()) {
								sendDelete(selection, entry.getKey(), entry.getValue());
							}
						}

					}
				}
			} catch (NoSuchAlgorithmException nsa) {
				nsa.getMessage();
			}

			return 0;
		}
	}

	public void sendDelete(String selection, int index, int replicaIndex){
		String port = EMULATORS.get(index);
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port) * 2);

			DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
			DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

			outputMessage.writeUTF("Delete," + selection);
			inputMessage.readUTF();

			outputMessage.flush();
			inputMessage.close();
			outputMessage.close();

		} catch (IOException e) {
			String replicaPort = EMULATORS.get(replicaIndex);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(replicaPort) * 2);

				DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

				outputMessage.writeUTF("Store," + selection + "," + "DeleteData" + "," + listOfGenHash.get(index));

				outputMessage.flush();
				outputMessage.close();
			} catch (IOException ioe){
				ioe.getMessage();
			}
		}

	}


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean lookUp(String id, String currentId, String predecessorHash){
		if(currentId.compareTo(id) >= 0 && predecessorHash.compareTo(id) < 0 && predecessorHash.compareTo(currentId) < 0)
			return true;
		else if(currentId.compareTo(id) <= 0 && predecessorHash.compareTo(id) < 0 && predecessorHash.compareTo(currentId) > 0)
			return true;
		else if(currentId.compareTo(id) >= 0 && predecessorHash.compareTo(id) > 0 && predecessorHash.compareTo(currentId) > 0)
			return true;
		else
			return false;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		synchronized (this) {

			String fileKey = values.getAsString("key");
			String fileValue = values.getAsString("value");

			try {
				String id = genHash(fileKey);

				if (wait){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException inte){
						inte.getMessage();
					}
				}

				if (listOfGenHash != null) {
					for (int i = 0; i < listOfGenHash.size(); i++) {
						TreeMap<Integer, Integer> IndicesMap = new TreeMap<Integer, Integer>();

						if (listOfGenHash.get(i).equals(firstHash)) {
							if (lookUp(id, listOfGenHash.get(i), listOfGenHash.get(4))) {
								IndicesMap.put(i, i + 1);
								IndicesMap.put(i + 1, i + 2);
								IndicesMap.put(i + 2, i + 1);
							}
						} else {
							if (lookUp(id, listOfGenHash.get(i), listOfGenHash.get(i - 1))) {
								if (i == listOfGenHash.size() - 1) {
									IndicesMap.put(i, 0);
									IndicesMap.put(0, 1);
									IndicesMap.put(1, 0);
								} else if (i == listOfGenHash.size() - 2) {
									IndicesMap.put(i, i + 1);
									IndicesMap.put(i + 1, 0);
									IndicesMap.put(0, i + 1);
								} else {
									IndicesMap.put(i, i + 1);
									IndicesMap.put(i + 1, i + 2);
									IndicesMap.put(i + 2, i + 1);
								}
							}
						}

						if(IndicesMap != null){
							for(TreeMap.Entry<Integer, Integer> entry : IndicesMap.entrySet()){
								sendInsert(fileKey, fileValue, entry.getKey(), entry.getValue());
							}
						}
					}
				}
			} catch (NoSuchAlgorithmException nsa) {
				Log.e(TAG, "No Such Algorithm Exception : " + nsa.getMessage());
			} catch (Exception e) {
				Log.e(TAG, "Can't insert values : " + e.getMessage());
			}

			Log.v("insert", values.toString());
			return uri;
		}
	}

	public void sendInsert(String key, String value, int index, int replicaIndex){
		String port = EMULATORS.get(index);
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port) * 2);

			DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
			DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

			outputMessage.writeUTF("Replicate," + key + "," + value);
			inputMessage.readUTF();

			outputMessage.flush();
			inputMessage.close();
			outputMessage.close();

		} catch (IOException e) {
			String replicaPort = EMULATORS.get(replicaIndex);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(replicaPort) * 2);

				DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

				outputMessage.writeUTF("Store," + key + "," + value + "," + listOfGenHash.get(index));

				outputMessage.flush();
				outputMessage.close();
			} catch (IOException ioe){
				ioe.getMessage();
			}
		}

	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		Context con = getContext();

		//Referred from PA2B
		TelephonyManager tel = (TelephonyManager) con.getSystemService(con.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf(Integer.parseInt(portStr) * 2);

		try{
            myId = genHash(portStr);

			HashedPortsMap.put("5554",genHash("5554"));
			HashedPortsMap.put("5556",genHash("5556"));
			HashedPortsMap.put("5558",genHash("5558"));
			HashedPortsMap.put("5560",genHash("5560"));
			HashedPortsMap.put("5562",genHash("5562"));

			listOfGenHash.add(genHash("5554"));
			listOfGenHash.add(genHash("5556"));
			listOfGenHash.add(genHash("5558"));
			listOfGenHash.add(genHash("5560"));
			listOfGenHash.add(genHash("5562"));

			Collections.sort(listOfGenHash);
			firstHash = listOfGenHash.get(0);

			for(int i=0; i<listOfGenHash.size(); i++){
				for(TreeMap.Entry<String,String> entry : HashedPortsMap.entrySet()) {
					if(entry.getValue().equals(listOfGenHash.get(i))){
						EMULATORS.add(entry.getKey());
					}
				}
			}

			for(int i=0; i<EMULATORS.size(); i++){
				if(EMULATORS.get(i).equals(portStr)){
					if(i == 0){
						successorPort = EMULATORS.get(1);
						predecessorPort = EMULATORS.get(4);
					} else if(i == 4){
						successorPort = EMULATORS.get(0);
						predecessorPort = EMULATORS.get(3);
					} else {
						successorPort = EMULATORS.get(i+1);
						predecessorPort = EMULATORS.get(i-1);
					}
				}
			}

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException io){
			Log.e(TAG, "Can't create ServerSocket");
		} catch (NoSuchAlgorithmException nsa){
			Log.e(TAG, "NoSuchAlgorithm exception in ServerSocket");
		}

		String dummyReturnValue = null;

		try{
			DataInputStream inputStream = new DataInputStream(getContext().openFileInput("Dummy"));
			dummyReturnValue = inputStream.readUTF();
		} catch (FileNotFoundException fnf){
			fnf.getMessage();
		} catch (IOException ioe){
			ioe.getMessage();
		}

		if(dummyReturnValue == null){
			try{
				DataOutputStream outputStream = new DataOutputStream(getContext().openFileOutput("Dummy", Context.MODE_PRIVATE));
				outputStream.writeUTF("Dummy");
				outputStream.flush();
				outputStream.close();
			} catch (FileNotFoundException fnf){
				fnf.getMessage();
			} catch (IOException ioe){
				ioe.getMessage();
			}
		} else if(dummyReturnValue != null) {
			wait = true;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
		}

		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		synchronized (this) {

			String[] strArray = new String[]{"key", "value"};
			Object[] strObject;
			MatrixCursor mCursor = new MatrixCursor(strArray);

			//Reference: https://developer.android.com/reference/android/view/View.html#getContext()
			Context context = getContext();

			try {
				if (selection.equals("@")) {
					for (String i : getContext().fileList()) {
						if (!i.equals("Dummy")) {
							DataInputStream inputStream = new DataInputStream(context.openFileInput(i));
							String str = inputStream.readUTF();
							if (str != null) {
								strObject = new Object[]{i, str};
								mCursor.addRow(strObject);
							}

							inputStream.close();
						} else {
							continue;
						}
					}

					return mCursor;
				} else if (selection.equals("*")) {
					Log.i(TAG, "In star query!");
					for (int i = 0; i < listOfGenHash.size(); i++) {
						String globalQueryString = QueryStar(i);

						Log.i(TAG, "Star string is : " + globalQueryString);

						if (globalQueryString != null) {
							String[] newArray = globalQueryString.split(",");

							int skip = 1;
							while (skip < newArray.length) {
								strObject = new Object[]{newArray[skip], newArray[skip + 1]};
								mCursor.addRow(strObject);
								skip += 2;
							}
						}
					}

					return mCursor;
				} else {

					String id = genHash(selection);

					if (wait){
						try {
							Thread.sleep(1000);
						} catch (InterruptedException inte){
							inte.getMessage();
						}
					}

					Log.i(TAG, "Querying ......................");
					if (listOfGenHash != null) {
						for (int i = 0; i < listOfGenHash.size(); i++) {
							List<Integer> listOfIndices = new ArrayList<Integer>();
							List<String> results = new ArrayList<String>();

							if (listOfGenHash.get(i).equals(firstHash)) {
								if (lookUp(id, listOfGenHash.get(i), listOfGenHash.get(listOfGenHash.size() - 1))) {
									Log.i(TAG, "Lookup true for first hash ......................");

									listOfIndices.add(i);
									listOfIndices.add(i + 1);
									listOfIndices.add(i + 2);

									String s1 = sendQuery(i, selection);
									String s2 = sendQuery(i + 1, selection);
									String s3 = sendQuery(i + 2, selection);

									Log.e(TAG, "String 1 : " + s1);
									Log.e(TAG, "String 2 : " + s2);
									Log.e(TAG, "String 3 : " + s3);

									if (s1 == null) {
										String[] splitString = s2.split(",");
										String[] strToAdd = new String[]{splitString[0], splitString[1]};
										mCursor.addRow(strToAdd);
									} else if (s2 == null || s3 == null) {
										String[] splitString = s1.split(",");
										String[] strToAdd = new String[]{splitString[0], splitString[1]};
										mCursor.addRow(strToAdd);
									} else if (s1.equals(s2) && s1.equals(s3)) {
										String[] splitString = s1.split(",");
										String[] strToAdd = new String[]{splitString[0], splitString[1]};
										mCursor.addRow(strToAdd);
									} else if ((s1.equals(s2) && !s1.equals(s3)) || (s1.equals(s3) && !s1.equals(s2))) {
										String[] splitString = s1.split(",");
										String[] strToAdd = new String[]{splitString[0], splitString[1]};
										mCursor.addRow(strToAdd);
									} else if ((s3.equals(s2) && !s3.equals(s1)) || (s3.equals(s1) && !s3.equals(s2))) {
										String[] splitString = s3.split(",");
										String[] strToAdd = new String[]{splitString[0], splitString[1]};
										mCursor.addRow(strToAdd);
									} else if (s2.equals(s3) && !s2.equals(s1)) {
										String[] splitString = s2.split(",");
										String[] strToAdd = new String[]{splitString[0], splitString[1]};
										mCursor.addRow(strToAdd);
									}

									return mCursor;

								}
							} else {
								if (lookUp(id, listOfGenHash.get(i), listOfGenHash.get(i - 1))) {

									Log.i(TAG, "Lookup true for non-first hash ......................");

									if (i == listOfGenHash.size() - 1) {
										listOfIndices.add(i);
										listOfIndices.add(0);
										listOfIndices.add(1);

										String s1 = sendQuery(i, selection);
										String s2 = sendQuery(0, selection);
										String s3 = sendQuery(1, selection);

										Log.e(TAG, "String 1 : " + s1);
										Log.e(TAG, "String 2 : " + s2);
										Log.e(TAG, "String 3 : " + s3);

										if (s1 == null) {
											String[] splitString = s2.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if (s2 == null || s3 == null) {
											String[] splitString = s1.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if (s1.equals(s2) && s1.equals(s3)) {
											String[] splitString = s1.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if ((s1.equals(s2) && !s1.equals(s3)) || (s1.equals(s3) && !s1.equals(s2))) {
											String[] splitString = s1.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if ((s3.equals(s2) && !s3.equals(s1)) || (s3.equals(s1) && !s3.equals(s2))) {
											String[] splitString = s3.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if (s2.equals(s3) && !s2.equals(s1)) {
											String[] splitString = s2.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										}

									} else if (i == listOfGenHash.size() - 2) {
										listOfIndices.add(i);
										listOfIndices.add(i + 1);
										listOfIndices.add(0);

 										String s1 = sendQuery(i, selection);
										String s2 = sendQuery(i + 1, selection);
										String s3 = sendQuery(0, selection);

										Log.e(TAG, "String 1 : " + s1);
										Log.e(TAG, "String 2 : " + s2);
										Log.e(TAG, "String 3 : " + s3);

										if (s1 == null) {
											String[] splitString = s2.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if (s2 == null || s3 == null) {
											String[] splitString = s1.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if (s1.equals(s2) && s1.equals(s3)) {
											String[] splitString = s1.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if ((s1.equals(s2) && !s1.equals(s3)) || (s1.equals(s3) && !s1.equals(s2))) {
											String[] splitString = s1.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if ((s3.equals(s2) && !s3.equals(s1)) || (s3.equals(s1) && !s3.equals(s2))) {
											String[] splitString = s3.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if (s2.equals(s3) && !s2.equals(s1)) {
											String[] splitString = s2.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										}

									} else {
										listOfIndices.add(i);
										listOfIndices.add(i + 1);
										listOfIndices.add(i + 2);

										String s1 = sendQuery(i, selection);
										String s2 = sendQuery(i + 1, selection);
										String s3 = sendQuery(i + 2, selection);

										Log.e(TAG, "String 1 : " + s1);
										Log.e(TAG, "String 2 : " + s2);
										Log.e(TAG, "String 3 : " + s3);

										if (s1 == null) {
											String[] splitString = s2.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if (s2 == null || s3 == null) {
											String[] splitString = s1.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if (s1.equals(s2) && s1.equals(s3)) {
											String[] splitString = s1.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if ((s1.equals(s2) && !s1.equals(s3)) || (s1.equals(s3) && !s1.equals(s2))) {
											String[] splitString = s1.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if ((s3.equals(s2) && !s3.equals(s1)) || (s3.equals(s1) && !s3.equals(s2))) {
											String[] splitString = s3.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										} else if (s2.equals(s3) && !s2.equals(s1)) {
											String[] splitString = s2.split(",");
											String[] strToAdd = new String[]{splitString[0], splitString[1]};
											mCursor.addRow(strToAdd);
										}
									}

									return mCursor;
								}
							}
						}

						return mCursor;
					}
				}
			} catch (FileNotFoundException e) {
				Log.e(TAG, "File not found exception while querying values : " + e.getMessage());
			} catch (IOException e) {
				Log.e(TAG, "IO exception while querying values : " + e.getMessage());
			} catch (Exception e) {
				Log.e(TAG, "Can't query values : " + e.getMessage());
			}

			Log.v("query", selection);
			return null;
		}
	}


	public String QueryStar(int i){
		try {
			Log.i(TAG, "Query to : " + Integer.parseInt(EMULATORS.get(i)) * 2);
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(EMULATORS.get(i)) * 2);
			DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
			DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

			outputMessage.writeUTF("Star,");

			String globalQueryString = inputMessage.readUTF();
			Log.i(TAG, "Received star string : " + globalQueryString);

			outputMessage.flush();
			inputMessage.close();
			outputMessage.close();

			return  globalQueryString;
		} catch (IOException ioe){
			ioe.getMessage();
		}

		return null;
	}

	public String sendQuery(int index, String selection){
		String portNum = EMULATORS.get(index);
		try {
			Log.i(TAG, "Send query to : " + EMULATORS.get(index));
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(portNum) * 2);

			DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
			DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

			outputMessage.writeUTF("Query,"+selection);

			String result = inputMessage.readUTF();

			Log.i(TAG, "Received query : " + result);

			outputMessage.flush();
			inputMessage.close();
			outputMessage.close();

			return  result;

		} catch (IOException ioe) {
			ioe.getMessage();
		}
		return  null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

		//Referred from OnTestClickListener.java
		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			Log.i(TAG, "Serversocket contents : "+serverSocket);

			while (true){
				try {
					Socket socket = serverSocket.accept();
					DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
					DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

					String message = inputMessage.readUTF();
					String[] splitMessage = message.split(",");

					if(splitMessage[0].equals("Delete")){
						getContext().deleteFile(splitMessage[1]);
					} else if(splitMessage[0].equals("Replicate")){
						ContentValues contentValues = new ContentValues();
						contentValues.put("key",splitMessage[1]);
						contentValues.put("value", splitMessage[2]);

						files.add(splitMessage[1]);

						synchronized (this) {
							DataOutputStream outputStream = new DataOutputStream(getContext().openFileOutput(splitMessage[1], Context.MODE_PRIVATE));
							outputStream.writeUTF(splitMessage[2]);
							outputStream.flush();
							outputStream.close();
						}
					} else if(splitMessage[0].equals("Query")){
						synchronized (this) {
							String returnString = "";
							if (splitMessage.length > 1 && (!splitMessage[1].isEmpty() || splitMessage[1] != null)) {
								DataInputStream inputStream = new DataInputStream(getContext().openFileInput(splitMessage[1]));
								String str = inputStream.readUTF();
								if (str != null) {
									returnString = splitMessage[1] + "," + str;
								}

								Log.i(TAG, "Returning query string : " + returnString);
								outputMessage.writeUTF(returnString);
							}
						}
					} else if(splitMessage[0].equals("Star")) {
						synchronized (this) {
							List<String> strC = new ArrayList<String>();

							String globalQueryString = "Star,";
							String[] strArray = new String[]{"key", "value"};
							MatrixCursor mCursor = new MatrixCursor(strArray);
							Object[] strObject;

							for (String i : getContext().fileList()) {
								if (!i.equals("Dummy")) {
									DataInputStream inputStream = new DataInputStream(getContext().openFileInput(i));
									String str = inputStream.readUTF();
									if (str != null) {
										strObject = new Object[]{i, str};
										mCursor.addRow(strObject);
									}
								} else {
									continue;
								}
							}

							int count = mCursor.getCount();
							int columnCount = mCursor.getColumnCount();

							//Reference : https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
							if (count > 0) {
								mCursor.moveToFirst();
								do {
									for (int i = 0; i < columnCount; i++) {
										strC.add(mCursor.getString(i));
									}
								} while (mCursor.moveToNext());

								if (strC != null) {
									for (String s : strC) {
										globalQueryString += s + ",";
									}
								}

								outputMessage.writeUTF(globalQueryString);
							}
						}
					} else if(splitMessage[0].equals("Store")) {
						synchronized (this) {
							Log.e(TAG, "STORE DATA");
							StoreMessagesOnFailure mo = null;
							mo = new StoreMessagesOnFailure(splitMessage[1], splitMessage[2], splitMessage[3], false);
							failureMessages.add(mo);
						}
					} else if(splitMessage[0].equals("Recovery")) {
						synchronized (this) {
							String data = "Keyvalue,";

							Log.e(TAG, "GET DATA");
							if (!failureMessages.isEmpty() && failureMessages != null) {
								for (StoreMessagesOnFailure miso : failureMessages) {
									if (!miso.mark && genHash(splitMessage[1]).equals(miso.failedHash)) {
										data += miso.key + "," + miso.value + ",";
										miso.mark = true;
									}
								}

								outputMessage.writeUTF(data);
							}
						}
					} else if(splitMessage[0].equals("Done")){
						wait = false;
					}
					outputMessage.flush();
					outputMessage.close();
					socket.close();

				} catch (IOException io){
					Log.e(TAG, "ClientTask IO Exception"+io.getMessage());
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {

			List<String> recoveryPorts = new ArrayList<String>();
			recoveryPorts.add(successorPort);
			recoveryPorts.add(predecessorPort);

			for(int i=0; i<recoveryPorts.size(); i++){
				try{
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(recoveryPorts.get(i)) * 2);

					DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
					DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

                    outputMessage.writeUTF("Recovery," + portStr);
					String recovered = inputMessage.readUTF();

					if(recovered.length() > 1){
						String[] rec = recovered.split(",");

						int skip = 1;
						while(skip < rec.length) {
							if(rec[skip + 1].equals("DeleteData")){
								Log.e(TAG, "DELETE DATA");
								getContext().deleteFile(rec[skip]);
							} else{
								Log.e(TAG, "WRITE DATA");

								Log.e(TAG, "Updating key value");
								DataOutputStream outputStream = new DataOutputStream(getContext().openFileOutput(rec[skip], Context.MODE_PRIVATE));
								outputStream.writeUTF(rec[skip + 1]);
								outputStream.flush();
								outputStream.close();
							}

							skip += 2;
						}

						outputMessage.writeUTF("Done");
					}

                    outputMessage.flush();
                    inputMessage.close();
                    outputMessage.close();
				} catch (IOException ioe){
					ioe.getMessage();
				}
			}

			return null;
		}
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}

class StoreMessagesOnFailure{

	String key = null;
	String value = null;
	String failedHash = null;
	Boolean mark = null;

	public StoreMessagesOnFailure(String key, String value, String failedHash, Boolean mark){
		this.key = key;
		this.value = value;
		this.failedHash = failedHash;
		this.mark = mark;
	}
}