package fs.common;


import java.util.Collections;
import java.util.Vector;

public class SortTokens implements Comparable<SortTokens> {
	private String token;
	private int weight;

	public SortTokens(String token, int weight) {
		this.weight = weight;
		this.token = token;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public String gettoken() {
		return token;
	}

	public void settoken(String token) {
		this.token = token;
	}
	
	@Override
	public int compareTo(SortTokens object) {
		// TODO Auto-generated method stub
		if(this.getWeight()<object.getWeight())
			return -1;
		else if(this.getWeight()>object.getWeight())
			return 1;
		else
	        return 0;		
	}

	public static void main(String args[]) {
		Vector<SortTokens> vec = new Vector<SortTokens>();
		SortTokens t1 = new SortTokens("aa", 3);
		SortTokens t2 = new SortTokens("bb", 1);
		SortTokens t3 = new SortTokens("cc", 4);
		SortTokens t4 = new SortTokens("dd", 5);
		vec.add(t1);
		vec.add(t2);
		vec.add(t3);
		vec.add(t4);
		Collections.sort(vec);
		for (int i = 0; i < vec.size(); i++)
			System.out.println(vec.get(i).gettoken() + ":"
					+ vec.get(i).getWeight());
	}

}
