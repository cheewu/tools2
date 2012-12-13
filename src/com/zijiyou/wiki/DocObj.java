package com.zijiyou.wiki;

public class DocObj {
	protected String id;
	protected String title;
	protected String type;
	protected String description;
	protected Double latitude;
	protected Double longitude;
	protected String content;
	protected String englishName;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public Double getLatitude() {
		return latitude;
	}
	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}
	public Double getLongitude() {
		return longitude;
	}
	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}
	public String getEnglishName() {
		return englishName;
	}
	public void setEnglishName(String englishName) {
		this.englishName = englishName;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public void clear() {
		id = null;
		title = null;
		type = null;
		description = null;
		latitude = 0.0;
		longitude = 0.0;
		content = null;
		englishName = null;
	}
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("[");
		if(id!=null) {
			sb.append("id=" + id);
		}
		sb.append(", ");
		if(title!=null) {
			sb.append("title=" + title);
		}
		sb.append(", ");
		if(type!=null) {
			sb.append("type=" + type);
		}
		sb.append(", ");
		if(latitude!=null) {
			sb.append("latitude=" + latitude);
		}
		sb.append(", ");
		if(longitude!=null) {
			sb.append("longitude=" + longitude);
		}
		sb.append(", ");
		if(englishName!=null) {
			sb.append("englishName=" + englishName);
		}
		sb.append(", ");
//		if(content!=null) {
//			sb.append("content=" + content);
//		}
		String s = sb.toString().trim();
		if(s.endsWith(",")) {
			s = s.substring(0, s.length()-1);
		}
		return s + "]";
	}
	

}
