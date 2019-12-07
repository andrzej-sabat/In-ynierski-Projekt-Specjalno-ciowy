package ips_project.ips.model;

public class ckMovie {
    private int movieId;
    private String title;
    private String tag;
    private String genre;

    public ckMovie(int movieId, String title, String tag, String genre) {
        this.movieId = movieId;
        this.title = title;
        this.tag = tag;
        this.genre = genre;
    }

    public ckMovie(int movieId, String title, String tag) {
        this.movieId = movieId;
        this.title = title;
        this.tag = tag;
    }

    public ckMovie() {

    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }
}
