package org.example;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import java.io.IOException;

public class Credentials {

    public static void main(String[] args) throws IOException {
        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
        credentials.refreshIfExpired();
        AccessToken accessToken = credentials.refreshAccessToken();

//        ServiceAccountCredentials credentials2 = (ServiceAccountCredentials) credentials;
//        AccessToken ac = credentials2.getAccessToken();
        System.out.println(accessToken);
    }
}
