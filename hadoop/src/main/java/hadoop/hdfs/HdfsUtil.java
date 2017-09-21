package hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtil {
	
	FileSystem fs = null;

	
	@Before
	public void init() throws Exception{
		
		//读取classpath下的xxx-site.xml 配置文件，并解析其内容，封装到conf对象中
		Configuration conf = new Configuration();
		
		//也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉配置文件中的读取的值
		conf.set("fs.defaultFS", "hdfs://weekend110:9000/");
		
		//根据配置信息，去获取一个具体文件系统的客户端操作实例对象
		fs = FileSystem.get(new URI("hdfs://weekend110:9000/"),conf,"hadoop");
		org.apache.hadoop.io.IOUtils.copyBytes(System.in,System.out,4096,true);
		
	}
	
	
	
	/**
	 * 上传文件，比较底层的写法
	 * 
	 * @throws Exception
	 */
	@Test
	public void upload() throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://weekend110:9000/");
		
		FileSystem fs = FileSystem.get(conf);
		
		Path dst = new Path("hdfs://weekend110:9000/aa/qingshu.txt");
		
		FSDataOutputStream os = fs.create(dst);
		
		FileInputStream is = new FileInputStream("c:/qingshu.txt");
		
		IOUtils.copy(is, os);
		

	}

	/**
	 * 上传文件，封装好的写法
	 * @throws Exception
	 * @throws IOException
	 */
	@Test
	public void upload2() throws Exception, IOException{
		
		fs.copyFromLocalFile(new Path("c:/qingshu.txt"), new Path("hdfs://weekend110:9000/aaa/bbb/ccc/qingshu2.txt"));
		
	}
	
	
	/**
	 * 下载文件
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void download() throws Exception {
		
		fs.copyToLocalFile(new Path("hdfs://weekend110:9000/aa/qingshu2.txt"), new Path("c:/qingshu2.txt"));

	}

	/**
	 * 查看文件信息
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 * 
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {

		// listFiles列出的是文件信息，而且提供递归遍历
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		
		while(files.hasNext()){
			
			LocatedFileStatus file = files.next();
			Path filePath = file.getPath();
			String fileName = filePath.getName();
			System.out.println(fileName);
			
		}
		
		System.out.println("---------------------------------");
		
		//listStatus 可以列出文件和文件夹的信息，但是不提供自带的递归遍历
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus status: listStatus){
			
			String name = status.getPath().getName();
			System.out.println(name + (status.isDirectory()?" is dir":" is file"));
			
		}
		
	}

	/**
	 * 创建文件夹
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, Exception {

		fs.mkdirs(new Path("/aaa/bbb/ccc"));
		
		
	}

	/**
	 * 删除文件或文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void rm() throws IllegalArgumentException, IOException {

		fs.delete(new Path("/aa"), true);
		
	}

	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://weekend110:9000/");
		
		FileSystem fs = FileSystem.get(conf);
		
		FSDataInputStream is = fs.open(new Path("/jdk-7u65-linux-i586.tar.gz"));
		
		FileOutputStream os = new FileOutputStream("c:/jdk7.tgz");
		
		IOUtils.copy(is, os);
	}
	
	
	
}