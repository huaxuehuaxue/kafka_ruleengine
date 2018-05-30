import java.util.HashMap;

public class Test1
{

	public static void main(String[] args)
	{
		HashMap<String, Integer> x = new HashMap<>();
		
		x.put("hello", 1);
		x.put("HELLO", 2);
		
		System.out.println("size: " + x.size());
		
		System.out.println("value: " + x.get("GAGA"));
	}

}
