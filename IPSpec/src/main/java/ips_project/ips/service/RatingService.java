package ips_project.ips.service;
import ips_project.ips.model.Rating;
import ips_project.ips.repository.RatingRepository;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.*;
import org.neo4j.ogm.config.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;


@Service
@Transactional
public class RatingService {

    @Autowired
    RatingRepository ratingRepository;

    @Transactional
    public Collection<Rating> findAllRatings() {
        return ratingRepository.getAllRatings();
    }

    @Transactional
    public void save(Rating rating) {
        ratingRepository.save(rating);
    }

    public Rating findById(Long id) { return ratingRepository.findById(id);}

    public void delete(Rating rating) {ratingRepository.delete(rating);}

    public List<Rating> findByRate(Integer rate) { return ratingRepository.findByRate(rate);}

    public  List<Rating> findByRateAndCount(Integer rate) { return  ratingRepository.findByRateAndCount(rate);}

    public List<Rating> sumRates() { return ratingRepository.sumRates();}

    @Transactional
    public void loadRatingCsvFromClickHouse() { ratingRepository.loadRatingCsvFromClickHouse();}



}






