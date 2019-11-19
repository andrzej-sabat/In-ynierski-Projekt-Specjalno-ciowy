package ips_project.ips.model;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;

@NodeEntity
public class Movie {

    @GraphId
    private Long graphId;
    private String id;
    private String title;
    private String genres;


    public Movie() {

    }

    public Long getGraphId() {
        return graphId;
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getGenres() {
        return genres;
    }
}
