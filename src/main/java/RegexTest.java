
public class RegexTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Case#: 125513".matches("Case\\s*#\\s*:\\s+\\d+"));
		
		System.out.println("Backline escalation for Case#: 123914 is now Awaiting Backline."
				.matches(".*Case\\s*#\\s*:\\s+\\d+.*\\r*"));
	}

}
