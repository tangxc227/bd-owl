package com.owl.s2sh.util;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:15 2018/12/24
 * @Modified by:
 */
public class SessionFactoryConfig {

    private static SessionFactory sessionFactory = null;

    static {
        Configuration configuration = new Configuration().configure();
        sessionFactory = configuration.buildSessionFactory();
    }

    private static SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public static Session openSession() {
        return getSessionFactory().openSession();
    }

    public static void close(Session session) {
        if (null != session) {
            session.close();
        }
    }

}
