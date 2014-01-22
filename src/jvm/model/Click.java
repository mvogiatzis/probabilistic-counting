package model;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Class representing the click data
 * @author michaelvogiatzis
 *
 */
public class Click {
/*
 * 
 * {
        "a": USER_AGENT, 
        "c": COUNTRY_CODE, # 2-character iso code
        "nk": KNOWN_USER,  # 1 or 0. 0=this is the first time we've seen this browser
        "g": GLOBAL_BITLY_HASH, 
        "h": ENCODING_USER_BITLY_HASH,
        "l": ENCODING_USER_LOGIN,
        "hh": SHORT_URL_CNAME,
        "r": REFERRING_URL,
        "u": LONG_URL,
        "t": TIMESTAMP,
        "gr": GEO_REGION,
        "ll": [LATITUDE, LONGITUDE],
        "cy": GEO_CITY_NAME,
        "tz": TIMEZONE # in http://en.wikipedia.org/wiki/Zoneinfo format
        "hc": TIMESTAMP OF TIME HASH WAS CREATED, 
        "al": ACCEPT_LANGUAGE http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.4 
    }
 * 
 */
	
	/*
	 * {
    "h": "1ciVGPa",
    "g": "1dq2pYm",
    "l": "asavins",
    "hh": "1.usa.gov",
    "u": "http:\/\/www.regulations.gov\/#!submitComment;D=IRS-2013-0038-0001",
    "r": "http:\/\/m.facebook.com\/l.php?u=http%3A%2F%2F1.usa.gov%2F1ciVGPa&h=9AQE-hw05&s=1&enc=AZOaYQsOueDtIu3KeOUuhLo9Ie9Ijng6HftGHiRNfsc-wKMR_SziC60emU68t4clsts",
    "a": "Mozilla\/5.0 (Linux; U; Android 4.1.2; en-us; SCH-I535 Build\/JZO54K) AppleWebKit\/534.30 (KHTML, like Gecko) Version\/4.0 Mobile Safari\/534.30",
    "t": 1390068412,
    "nk": 0,
    "hc": 1389712197,
    "_id": "52dac2bc-001ae-03d83-441cf10a",
    "al": "en-US",
    "c": "US",
    "tz": "America\/New_York",
    "gr": "GA",
    "cy": "Mcdonough",
    "ll": [33.447300, -84.146900]
	  }
	 */
	
	private String countryCode;
	private String longURL;
	/**
	 * The referring url
	 */
	private String refURL;
	
	public String getCountryCode(){
		return countryCode;
	}
	
	public void setCountryCode(String countryCode){
		this.countryCode = countryCode;
	}
	
	public String getLongURL(){
		return longURL;
	}
	
	public void setLongURL(String longURL){
		this.longURL = longURL;
	}
	
	public String getRefURL(){
		return refURL;
	}
	
	/**
	 * Sets the referring url
	 * @param refURL
	 */
	public void setRefURL(String refURL){
		this.refURL = refURL;
	}

	/**
	 * Populate the object using the json string
	 * @param json
	 */
	public void populate(String json) {
		JSONObject obj=(JSONObject) JSONValue.parse(json);
	    this.longURL = (String) obj.get("u");
	    Object countryCode = (Object) obj.get("c");
	    if (countryCode!=null){
	    	this.countryCode = (String) countryCode;
	    }
	    
	    Object refUrl = (Object) obj.get("r");
	    if (refUrl!=null){
	    	this.refURL = (String) refUrl;
	    }
		
	}
	
}
