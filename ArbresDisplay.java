package filedisplay;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

//Written by Gregory ISLAS
//Class takes in the file arbres.csv and outputs a formatted version containing the fields
//year and height of each tree. Outputs white space for missing fields.
public class ArbresDisplay {

	public static void main(String[] args) throws IOException {
		int count = 0; 
		Path filename = new Path("data/arbres.csv"); //assumes file exists in directory data, which is in same directory as code
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		String output_path = "arbres_yr_ht.txt";
		FSDataOutputStream out = fs.create(new Path(output_path));
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String header = "YEAR\tHEIGHT\n"; //header
			String line = br.readLine();
			while (line !=null){
				// Process of the current line
				if(count == 0) { //write the header; also, first line of file contains a header, so dont write that line to new file
					out.writeBytes(header);
				}
				else {
					//System.out.println(line);
					String[] line_split = line.split(";");
					String year = line_split[5];
					if(year.isEmpty()) { //for more aesthetically pleasing formatting
						year = "\t";
					}
					String towrite = year + "\t" + line_split[6] + "\n"; //concat the fields
					out.writeBytes(towrite); //write them to output
				// go to the next line
					
				}
				line = br.readLine();
				count++;
			}
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
			out.close();
		}

	}

}
