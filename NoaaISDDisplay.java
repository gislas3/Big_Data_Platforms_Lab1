package filedisplay;



import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


//Class takes in the file isd-history.txt and outputs a formatted version containing the fields
//USAF code, name, country, and the elevation of each station. Outputs white space for missing fields.
public class NoaaISDDisplay {

	public static void main(String[] args) throws IOException {
		
		int count = 0; //keep track what line number of file on
		Path filename = new Path("data/isd-history.txt"); //assumes file exists in directory data, which is in same folder as the program
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		String output_path = "isd-history-formatted.txt"; //where file will be saved to
		FSDataOutputStream out = fs.create(new Path(output_path));
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String header = "USAF\tSTATION NAME\tCTRY\tELEV(M)+\n";
			String line = br.readLine();
			while (line !=null){
				// Process of the current line
				if(count == 0) { //write the header
					out.writeBytes(header);
				}
				else if(count >= 22) {
					//System.out.println(line);
					//get each field - assumes that they will always be located in the specified places
					String usaf_code = line.substring(0, 6).trim();
					if(usaf_code.isEmpty()) {
						usaf_code = "\t";
					}
					String name = line.substring(13, 13+29).trim();
					if(name.isEmpty()) {
						name = "\t";
					}
					String fips = line.substring(43, 43+2).trim();
					if(fips.isEmpty()) {
						fips = "\t";
					}
					String alt = line.substring(74, 74+7).trim();
					
					String total = usaf_code + "\t" + name + "\t\t"+ fips + "\t" + alt +  "\n"; //concatenate the string
					out.writeBytes(total); //write the string to the new stream
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

