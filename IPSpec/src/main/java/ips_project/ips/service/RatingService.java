package ips_project.ips.service;
import ips_project.ips.model.Movie;
import ips_project.ips.model.Rating;
import ips_project.ips.repository.RatingRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;


@Service
public class RatingService {

    @Autowired
    RatingRepository ratingRepository;

    @Transactional
    public Collection<Rating> getAll() {
        return ratingRepository.getAllRatings();
    }

    @Transactional
    public void addRating(Rating rating) {
        ratingRepository.save(rating);
    }
}
