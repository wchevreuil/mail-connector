package com.cloudera.tools;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.cloudera.connector.mail.MailBoxReader;
import com.google.api.client.repackaged.org.apache.commons.codec.binary.Base64;
import com.google.api.client.repackaged.org.apache.commons.codec.binary.StringUtils;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;

public class InstanceExtender {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		final Gmail service = MailBoxReader.getGmailService();
		
		Gmail.Users.Messages.List messagesList = service.users().messages().list("me").setQ("from:isebot Will Expire");
		
		ListMessagesResponse listMessagesResponse = messagesList.execute();
		
		for(Message m : listMessagesResponse.getMessages()){
			
			Message msg = service.users().messages().get("me", m.getThreadId()).execute();
			
			if(msg.getInternalDate() >= (System.currentTimeMillis() - (24*60*60*1000))) {
				
				System.out.println(msg.getInternalDate());
				
				String mailContent = StringUtils.newStringUtf8(Base64.decodeBase64(msg.getPayload().getBody().getData()));
				
				URL url = new URL(mailContent.substring(mailContent.indexOf("http")));
				
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				
				conn.setRequestMethod("GET");
				
				int responseCode = conn.getResponseCode();
				
				System.out.println("RESPONSE: " + responseCode);
				
				BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				
				String line = null;
				
				while((line = reader.readLine()) != null){
					
					System.out.println(line);
					
				}
				
				reader.close();
				
			}

		}
	}

}
