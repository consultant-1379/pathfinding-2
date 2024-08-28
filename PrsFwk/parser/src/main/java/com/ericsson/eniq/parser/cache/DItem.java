package com.ericsson.eniq.parser.cache;

public class DItem implements Comparable {

	private final String dataName;
	private final int colNumber;
	private final String dataID;
	private final String pi;
	private final String dataType;
	private final int dataSize;
	private final int dataScale;
	private final int isCounter;

	public DItem(final String dataName, final int colNumber, final String dataID, final String pi,
			final String datatype, final int datasize, final int datascale) {
    this(dataName, colNumber, dataID, pi, datatype, datasize, datascale, 0);
	}
	
	public DItem(final String dataName, final int colNumber, final String dataID, final String pi,
			final String datatype, final int datasize, final int datascale, final int isCounter) {
		super();
		this.dataName = dataName;
		this.colNumber = colNumber;
		this.dataID = dataID;
		this.pi = pi;
		this.dataType = datatype;
		this.dataSize = datasize;
		this.dataScale = datascale;
		this.isCounter = isCounter;
	}

	public DItem(final String dname, final int colno, final String did, final String pinst) {
    this(dname, colno, did, pinst, null, 0, 0, 0);
	}

	public int getColNumber() {
		return colNumber;
	}

	public String getDataID() {
		return dataID;
	}

	public String getDataName() {
		return dataName;
	}

	public String getProcessInstruction() {
		return pi;
	}

	public String getDataType() {
		return dataType;
	}

	public int getDataSize() {
		return dataSize;
	}

	public int getDataScale() {
		return dataScale;
	}
	
	public int getIsCounter() {
		return isCounter;
	}

	public int compareTo(final Object o) {
		if (o instanceof DItem) {
			final DItem k = (DItem) o;
			if (colNumber < k.getColNumber()) {
				return -1;
      } else if (colNumber > k.getColNumber()) {
				return 1;
      }
		}
		return 0;
	}
	
	@Override
	public String toString() {
		return "DItem [dataName=" + dataName + ", colNumber=" + colNumber + "]";
	}

}

