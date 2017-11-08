package com.test2data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SparkJSONDemo {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkIO");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
//        readJsonTest(sc);
        writeJsonTest(sc);
        sc.stop();
        sc.close();
    }

    //读JSON
    static void readJsonTest(JavaSparkContext sc){
        JavaRDD<String> input = sc.textFile("E:\\git\\java\\spark\\src\\main\\resources\\song.json");
        JavaRDD<Mp3Info> result = input.mapPartitions(new ParseJson());
        result.foreach(x->System.out.println(x));
    }
    //写JSON
    static void writeJsonTest(JavaSparkContext sc){
        JavaRDD<String> input = sc.textFile("E:\\git\\java\\spark\\src\\main\\resources\\song.json");
        JavaRDD<Mp3Info> result = input.mapPartitions(new ParseJson()).
                filter(
                        x->x.getAlbum().equals("怀旧专辑")
                );
        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
        result.foreach(x->System.out.println(x));
        formatted.saveAsTextFile("E:\\git\\java\\spark\\src\\main\\resources\\oldsong.json");
    }
}

class ParseJson implements FlatMapFunction<Iterator<String>, Mp3Info>, Serializable {
    public Iterator<Mp3Info> call(Iterator<String> lines) throws Exception {
        ArrayList<Mp3Info> people = new ArrayList<Mp3Info>();
        ObjectMapper mapper = new ObjectMapper();
        while (lines.hasNext()) {
            String line = lines.next();
            try {
                people.add(mapper.readValue(line, Mp3Info.class));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return people.iterator();
    }
}

class WriteJson implements FlatMapFunction<Iterator<Mp3Info>, String> {
    public Iterator<String> call(Iterator<Mp3Info> song) throws Exception {
        ArrayList<String> text = new ArrayList<String>();
        ObjectMapper mapper = new ObjectMapper();
        while (song.hasNext()) {
            Mp3Info person = song.next();
            text.add(mapper.writeValueAsString(person));
        }
        return text.iterator();
    }
}

class Mp3Info implements Serializable{
    /*
{"name":"上海滩","singer":"叶丽仪","album":"香港电视剧主题歌","path":"mp3/shanghaitan.mp3"}
{"name":"一生何求","singer":"陈百强","album":"香港电视剧主题歌","path":"mp3/shanghaitan.mp3"}
{"name":"红日","singer":"李克勤","album":"怀旧专辑","path":"mp3/shanghaitan.mp3"}
{"name":"爱如潮水","singer":"张信哲","album":"怀旧专辑","path":"mp3/airucaoshun.mp3"}
{"name":"红茶馆","singer":"陈惠嫻","album":"怀旧专辑","path":"mp3/redteabar.mp3"}
     */
    private String name;
    private String album;
    private String path;
    private String singer;

    public String getSinger() {
        return singer;
    }
    public void setSinger(String singer) {
        this.singer = singer;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getAlbum() {
        return album;
    }
    public void setAlbum(String album) {
        this.album = album;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }
    @Override
    public String toString() {
        return "Mp3Info [name=" + name + ", album="
                + album + ", path=" + path + ", singer=" + singer + "]";
    }
}

/*
{"name":"上海滩","singer":"叶丽仪","album":"香港电视剧主题歌","path":"mp3/shanghaitan.mp3"}
{"name":"一生何求","singer":"陈百强","album":"香港电视剧主题歌","path":"mp3/shanghaitan.mp3"}
{"name":"红日","singer":"李克勤","album":"怀旧专辑","path":"mp3/shanghaitan.mp3"}
{"name":"爱如潮水","singer":"张信哲","album":"怀旧专辑","path":"mp3/airucaoshun.mp3"}
{"name":"红茶馆","singer":"陈惠嫻","album":"怀旧专辑","path":"mp3/redteabar.mp3"}
 */