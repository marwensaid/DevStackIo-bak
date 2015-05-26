package com.devstackio.maven.application.utils;

import java.io.*;
import sun.audio.AudioPlayer;
import sun.audio.AudioStream;

/**
 *
 * @author destackio [devstackioweb]
 */
public class BlitzUtils {

    private String Filename = "wav/tbardy00.wav";
    
    public static void main(String[] args ) {
        BlitzUtils instance = new BlitzUtils();
        instance.playBattlecruiserSound();
    }

    public void playBattlecruiserSound() {
        
        try {
            InputStream in;
            try {
                in = new BufferedInputStream(new FileInputStream(new File(
                        getClass().getClassLoader()
                        .getResource( Filename ).getPath())));
                AudioStream audioStream = new AudioStream(in);
                AudioPlayer.player.start(audioStream);
                Thread.sleep(3000);
                AudioPlayer.player.stop(audioStream);
                in.close();
                
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

}
