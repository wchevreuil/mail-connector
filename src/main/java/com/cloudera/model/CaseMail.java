package com.cloudera.model;

import com.amazonaws.util.StringUtils;

public class CaseMail {

	private String id;

	private String component;

	private String subComponent;

	private String account;

	private String subject;

	private String description;
	
	private Long timestamp;

	public CaseMail(String mail, Long timestamp){
		
		//parse all info from the raw text mail
		
		try{
			String[] lines = mail.split("\n");
		
			for (String line: lines){
				
				if(line.matches("Case\\s*#\\s*:\\s+\\d+.*\\r*")){
					this.id = line.split(":")[1].trim().split(" ")[0].trim();
					System.out.println(this.id);
				}else if(line.matches("Case Subject.*\\r*")){
					this.subject = line.split(":")[1].trim();
				}else if(line.contains("Component")){
					this.component = line.split(":")[1].trim();
				}else if(line.contains("Sub-component")){
					this.subComponent = line.split(":")[1].trim();
				}else if(line.contains("Account")){
					this.account = line.split(":")[1].trim();
				}
				
			}
			
			this.description = mail;
			this.timestamp = timestamp;
			
		}catch(Exception e){
			throw new IllegalArgumentException("Not a case.");
		}
		
		if(StringUtils.isNullOrEmpty(this.id)){
			throw new IllegalArgumentException("No processing exception, but still not a case.");
		}
		
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getComponent() {
		return component;
	}

	public void setComponent(String component) {
		this.component = component;
	}

	public String getSubComponent() {
		return subComponent;
	}

	public void setSubComponent(String subComponent) {
		this.subComponent = subComponent;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "CaseMail [id=" + id + ", component=" + component + ", subComponent=" + subComponent + ", account="
				+ account + ", subject=" + subject + ", description=" + description + "]";
	}

}
