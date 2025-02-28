package neuedu.chargerDataAnalysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ChargerReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalTime =0.0;

        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            totalTime += Double.parseDouble(iterator.next().toString());
        }

        context.write(key, new Text(String.valueOf(totalTime)));
    }
}
