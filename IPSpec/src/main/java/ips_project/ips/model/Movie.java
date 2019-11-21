package ips_project.ips.model;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import java.util.ArrayList;
import java.util.List;

@NodeEntity
public class Movie {

    @GraphId
    private Long id;
    private String title;
    private String genres;

    @Relationship(type = "PENDING")
    private List<Rating> rating = new ArrayList<>();


    public Movie() {

    }


    public Long getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getGenres() {
        return genres;
    }

    public List<Rating> getRating() {
        return rating;
    }

    public void setRating(List<Rating> rating) {
        this.rating = rating;
    }
}
