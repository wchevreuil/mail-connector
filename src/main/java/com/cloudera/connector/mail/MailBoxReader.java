package com.cloudera.connector.mail;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import com.cloudera.connector.hbase.HBaseDAO;
import com.cloudera.model.CaseMail;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.repackaged.org.apache.commons.codec.binary.Base64;
import com.google.api.client.repackaged.org.apache.commons.codec.binary.StringUtils;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;

public class MailBoxReader {

	/** Application name. */
	private static final String APPLICATION_NAME = "ClientMailReader";

	/** Directory to store user credentials for this application. */
	private static final java.io.File DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"),
			".credentials/gmail-java-quickstart");

	/** Global instance of the {@link FileDataStoreFactory}. */
	private static FileDataStoreFactory DATA_STORE_FACTORY;

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	/** Global instance of the HTTP transport. */
	private static HttpTransport HTTP_TRANSPORT;

	private static int responseCount = 0;

	/**
	 * Global instance of the scopes required by this quickstart.
	 *
	 * If modifying these scopes, delete your previously saved credentials at
	 * ~/.credentials/gmail-java-quickstart
	 */
	private static final List<String> SCOPES = Arrays.asList(GmailScopes.MAIL_GOOGLE_COM);

	static {
		try {
			HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Creates an authorized Credential object.
	 * 
	 * @return an authorized Credential object.
	 * @throws IOException
	 */
	public static Credential authorize() throws IOException {
		// Load client secrets.
		InputStream in = MailBoxReader.class.getResourceAsStream("/client_id.json");
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

		// Build flow and trigger user authorization request.
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY,
				clientSecrets, SCOPES).setDataStoreFactory(DATA_STORE_FACTORY).setAccessType("offline").build();
		Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("wellingtonmailboxreader");
		System.out.println("Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());
		return credential;
	}

	/**
	 * Build and return an authorized Gmail client service.
	 * 
	 * @return an authorized Gmail client service
	 * @throws IOException
	 */
	public static Gmail getGmailService() throws IOException {
		Credential credential = authorize();
		return new Gmail.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME).build();
	}

	public static void main(String[] args) throws Exception {
		// Build a new authorized API client service.
		final Gmail service = getGmailService();

		BatchRequest batch = service.batch();

		// Print the labels in the user's account.
		String user = "me";

		Gmail.Users.Messages.List messagesList = service.users().messages().list(user);

		ListMessagesResponse listMessagesResponse = messagesList.execute();
	
		final HBaseDAO dao = new HBaseDAO();

		listMessagesResponse = (args.length<1) ? messagesList.execute() : messagesList.setPageToken(args[0]).execute();

		List<Message> messages = listMessagesResponse.getMessages();

		int requestsCount = 0;

		while (messages != null) {

			for (Message m : messages) {

				service.users().messages().get(user, m.getThreadId()).queue(batch, new JsonBatchCallback<Message>() {

					public void onSuccess(Message msg, HttpHeaders arg1) throws IOException {

						String mailContent = StringUtils
								.newStringUtf8(Base64.decodeBase64(msg.getPayload().getBody().getData()));

						Long timestamp = msg.getInternalDate();

						try {
							
							synchronized (service) {
								
								responseCount++;
								
								CaseMail caseMail = new CaseMail(mailContent, timestamp);

								dao.insertCaseMail(caseMail);

								System.out.println("caching email for case " + caseMail.getId());

								System.out.println(responseCount);
							}
							
						} catch (IllegalArgumentException e) {
							System.out.println("Not a case mail!!!!");
							// System.out.println(mailContent);
						}

					}

					@Override
					public void onFailure(GoogleJsonError arg0, HttpHeaders arg1) throws IOException {
						// TODO Auto-generated method stub
						synchronized (service) {
							responseCount++;
						}

						System.out.println("got all responses back. Calling dao.flush()");
					}
				});

				if (requestsCount == 100) {
					// System.out.println("flushing 100 cases to hbase...");
					// dao.flush();
					System.out.println("bacth request of 100 messages payload");
					
					Long initTime = System.currentTimeMillis();
					
					try{
						
						batch.execute();
					
					}catch(Exception e){
						
						e.printStackTrace();
						
						synchronized(service){
						
							service.wait(1000);
						
						}
					
						batch.execute();
					
					}
					// System.out.println("time taken to flush content: "+
					// (System.currentTimeMillis()-initTime));
					requestsCount = 0;
					
					synchronized (service) {
					
						while (responseCount < 100) {
						
							service.wait(100);
						
						}
						
						System.out.println("batch resquests took " + (System.currentTimeMillis() - initTime) + " millis to to process.");
						
						responseCount = 0;
						
						initTime = System.currentTimeMillis();
						
						dao.flush();
						
						System.out.println("dao.flush() took " + (System.currentTimeMillis()-initTime));

					}
				}

				requestsCount++;

				System.out.println("--------");
			}

			String pageToken = listMessagesResponse.getNextPageToken();

			System.out.println("Current page token: " + pageToken);
			
			if (pageToken != null) {

				listMessagesResponse = messagesList.setPageToken(pageToken).execute();

				messages = listMessagesResponse.getMessages();

			} else {

				break;
			}
		}
		dao.flush();
		dao.close();

	}

}
