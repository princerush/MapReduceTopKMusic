package org.apache.hadoop.mapreduce.app.topmusic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TopMusicWritable implements WritableComparable<TopMusicWritable>{
	private String songType;
	private String songName;
	private Long playTimes;
	public TopMusicWritable() {}
	public TopMusicWritable(String songLang, String songName, Long playTimes) {
		this.set(songLang, songName, playTimes);
	}
	public void set(String songLang, String songName, Long playTimes) {
		this.songType = songLang;
		this.songName = songName;
		this.playTimes = playTimes;
	}
	public String getSongType() {
		return songType;
	}
	public String getSongName() {
		return songName;
	}
	public Long getPlayTimes() {
		return playTimes;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(songType);
		out.writeUTF(songName);
		out.writeLong(playTimes);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.songType = in.readUTF();
		this.songName = in.readUTF();
		this.playTimes = in.readLong();
	}
	@Override
	public int compareTo(TopMusicWritable o) {
		int minus = this.getPlayTimes().compareTo(o.getPlayTimes());
		if(minus != 0) {
			return -minus;
		}
		return -(this.getSongType().compareTo(o.getSongType()));
	}
	@Override
	public String toString() {
		return songType+"\t"+songName+"\t"+playTimes;
	}
	@Override
	public int hashCode() {
		return super.hashCode();
	}
	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}	
}
