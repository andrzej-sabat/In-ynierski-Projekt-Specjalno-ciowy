package ips_project.ips.resource;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

@RestController
public class HomeResource {

    @RequestMapping(value={"/", "/home"}, method = RequestMethod.GET)
    public ModelAndView home(){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("/home");
        return modelAndView;
    }

    @RequestMapping(value={"/neo4j_main"}, method = RequestMethod.GET)
    public ModelAndView neo4j_main(){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("/neo4j_main");
        return modelAndView;
    }

    @RequestMapping(value={"/ck_main"}, method = RequestMethod.GET)
    public ModelAndView ck_main(){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("/ck_main");
        return modelAndView;
    }
}
