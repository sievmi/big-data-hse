import java.util.Arrays;
import java.util.Random;

/**
 * Created by sievmi on 25.11.18
 */
public class CacheTimer {

    //L1 Cache:	32k/32k x2	L2/L3 Cache:	256k x2, 3 MB*
    // 256 int'ов это 1 кбайт

    public static void main(String[] args) {
        int measuresCount = 100000000;
        Random r = new Random();
        int[] arr = new int[100000000];
        Arrays.fill(arr, 2);

        for (int arraySize = 10; arraySize <= 100000000; arraySize *= 10) {
            long start = System.nanoTime();
            for (int j = 0; j < measuresCount; j++) {
                int y = Math.abs(r.nextInt() % arraySize);
                int x = arr[y];
            }
            System.out.println("Array size = " + arraySize + " time  = " + (System.nanoTime() - start + 0.0) / measuresCount + " ns");
        }


    }
}
