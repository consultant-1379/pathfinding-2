package com.ericsson.eniq.loadfilebuilder.outputstream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoadFileRepo {
	
	private static final Map<String, ILoadFile> fileRepo = new ConcurrentHashMap<>();
	
	private LoadFileRepo() {
		
	}
	
	public static ILoadFile getLoadFile(String outDir, String folderName){
		ILoadFile loadFile = fileRepo.get(folderName);
		if (loadFile == null) {
			loadFile = new LoadFile(outDir, folderName);
			fileRepo.put(folderName, loadFile);
		}
		return loadFile;
	}

}
