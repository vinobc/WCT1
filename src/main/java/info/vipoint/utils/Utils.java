package info.vipoint.utils;

public class Utils {
	public static void waitForSeconds(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			
		}
	}
	
	public static void waitForMillis(long ms) {
		try {
			Thread.sleep(ms);
		}catch(InterruptedException e) {
			
		}
	}

}
