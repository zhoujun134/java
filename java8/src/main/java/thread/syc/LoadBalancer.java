package thread.syc;

public interface LoadBalancer {
	void updateCandidate(final Candidate candidate);
	Endpoint nextEndpoint();
}
