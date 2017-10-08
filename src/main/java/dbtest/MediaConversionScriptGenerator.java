package dbtest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Use ffmpeg to shrink down media files.  The best combination I've found is H264 @ 1500 bit rate with 192K MP3 audio.
 * @author wojtek
 *
 */
public class MediaConversionScriptGenerator {

	public static void main(String[] args) throws IOException {
		long size = 0;
		
		List<String> dirctories = new ArrayList<String>();
		dirctories.add("/media/wojtek/media/movies/Batman");
		
		
		StringBuffer convertCommand = new StringBuffer();
		convertCommand.append("#!/bin/bash" + "\n");
		
		String command = "ffmpeg -i ";
		String videoAndAudio = " -c:v libx264 -b:v 1500k -c:a mp3 -b:a 192k";
		
		for (String directoryName : dirctories) {
			File directory = new File(directoryName);
			
			System.out.println("Looking at directory: " + directory.getAbsolutePath());
			File[] listFiles = directory.listFiles();
			
			if(listFiles == null) {
				System.out.println("No files.");
				continue;
			} else {
				System.out.println("Number of files: " + listFiles.length);
			}
			
			for (File file : listFiles) {
				if(!file.exists()) {
					System.out.println("File: " + file.getAbsolutePath() + " does not exit.");
					continue;
				}
				
				// Get the number of bytes in the file
				long sizeInBytes = file.length();
				
				//transform in MB
				size = size + (sizeInBytes / (1024 * 1024));
				
				String nameWithoutExtension = file.getName().substring(0, (file.getName().length() - 4));
				
				String convertedFileName = converFileName(nameWithoutExtension);
				
				convertCommand.append(command);
				convertCommand.append(file.getParent() + "/" + convertedFileName + ".mkv");
				convertCommand.append(videoAndAudio);
				convertCommand.append(" " + file.getParent() + "/" + convertedFileName + ".mp4");
				convertCommand.append("\n");
			}
		}
		
		System.out.println("Total Size: " + size + "MB");
		System.out.println(convertCommand.toString());

	}

	private static String converFileName(String name) {
		StringBuffer newName = new StringBuffer();
		StringTokenizer st = new StringTokenizer(name);

		while (st.hasMoreElements()) {
			newName.append(st.nextElement());
			newName.append("\\ ");
		}
		
		String outputwithoutquotes = newName.toString().replace("'", "\\'");
		
		return outputwithoutquotes.substring(0, outputwithoutquotes.toString().length() - 2);
	}
}
