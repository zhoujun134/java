package thread.syc;

import java.io.InputStream;

public class Request {

	private final long transactionId;
	private final int transactionType;
	private InputStream in;

	public Request(long transactionId, int transactionType) {
		this.transactionId = transactionId;
		this.transactionType = transactionType;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public int getTransactionType() {
		return transactionType;
	}

	public InputStream getIn() {
		return in;
	}

	public void setIn(InputStream in) {
		this.in = in;
	}

}
