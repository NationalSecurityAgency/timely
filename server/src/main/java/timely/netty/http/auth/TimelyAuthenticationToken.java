package timely.netty.http.auth;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyPrincipal;
import timely.auth.TimelyUser;
import timely.auth.util.DnUtils;
import timely.auth.util.HttpHeaderUtils;

public class TimelyAuthenticationToken extends PreAuthenticatedAuthenticationToken {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TimelyAuthenticationToken.class);
    public static final String PROXIED_ENTITIES_HEADER = "x-proxiedentitieschain";
    public static final String PROXIED_ISSUERS_HEADER = "x-proxiedissuerschain";

    private Multimap<String, String> httpHeaders = null;
    private X509Certificate clientCert = null;
    private TimelyPrincipal timelyPrincipal;

    public TimelyAuthenticationToken(String subjectDn, Object clientCert, Multimap<String, String> httpHeaders) {
        super(subjectDn, clientCert);
        if (httpHeaders == null) {
            this.httpHeaders = HashMultimap.create();
        } else {
            this.httpHeaders = httpHeaders;
        }
        if (clientCert instanceof X509Certificate) {
            this.clientCert = (X509Certificate) clientCert;
        }
        String issuerDn = clientCert == null ? null : this.clientCert.getIssuerDN().getName();
        String proxiedEntities;
        String proxiedIssuers;
        try {
            proxiedEntities = HttpHeaderUtils.getSingleHeader(httpHeaders, PROXIED_ENTITIES_HEADER, true);
            proxiedIssuers = HttpHeaderUtils.getSingleHeader(httpHeaders, PROXIED_ISSUERS_HEADER, true);
        } catch (IllegalArgumentException e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
        if (proxiedEntities != null && proxiedIssuers == null) {
            LOG.error(PROXIED_ENTITIES_HEADER + " supplied, but missing " + PROXIED_ISSUERS_HEADER);
            throw new IllegalArgumentException(
                    PROXIED_ENTITIES_HEADER + " supplied, but missing " + PROXIED_ISSUERS_HEADER);
        }
        List<SubjectIssuerDNPair> entities = extractEntities(subjectDn, issuerDn, proxiedEntities, proxiedIssuers);
        long now = System.currentTimeMillis();
        List<TimelyUser> timelyUsers = new ArrayList<>();
        for (SubjectIssuerDNPair ent : entities) {
            TimelyUser.UserType userType = DnUtils.isServerDN(ent.subjectDN()) ? TimelyUser.UserType.SERVER
                    : TimelyUser.UserType.USER;
            timelyUsers.add(new TimelyUser(ent, userType, null, null, null, now));
        }
        timelyPrincipal = new TimelyPrincipal(timelyUsers);
        LOG.trace("Created TimelyAuthenticationToken: {}", timelyPrincipal.getName());
    }

    public TimelyAuthenticationToken(String subjectDn, String issuerDn, Object clientCert) {
        super(subjectDn, clientCert);
        if (clientCert instanceof X509Certificate) {
            this.clientCert = (X509Certificate) clientCert;
        }
        TimelyUser.UserType userType = DnUtils.isServerDN(subjectDn) ? TimelyUser.UserType.SERVER
                : TimelyUser.UserType.USER;
        TimelyUser timelyUser = new TimelyUser(SubjectIssuerDNPair.of(subjectDn, issuerDn), userType, null, null, null,
                System.currentTimeMillis());
        timelyPrincipal = new TimelyPrincipal(timelyUser);
        LOG.trace("Created TimelyAuthenticationToken: {}", timelyPrincipal.getName());
    }

    private List<SubjectIssuerDNPair> extractEntities(String subjectDn, String issuerDn, String proxiedEntities,
            String proxiedIssuers) {
        List<SubjectIssuerDNPair> entities = new ArrayList<>();
        entities.add(SubjectIssuerDNPair.of(subjectDn, issuerDn));
        if (proxiedEntities != null) {
            String[] subjects = DnUtils.splitProxiedDNs(proxiedEntities, true);
            if (proxiedIssuers == null) {
                throw new IllegalArgumentException("Proxied issuers must be supplied if proxied subjects are supplied");
            }
            String[] issuers = DnUtils.splitProxiedDNs(proxiedIssuers, true);
            if (subjects.length != issuers.length) {
                throw new IllegalArgumentException("Proxied subjects and issuers don't match up. Subjects="
                        + proxiedEntities + " , Issuers=" + proxiedIssuers);
            }

            for (int i = 0; i < subjects.length; ++i) {
                entities.add(SubjectIssuerDNPair.of(subjects[i], issuers[i]));
            }
        }
        return entities;
    }

    public Multimap<String, String> getHttpHeaders() {
        return httpHeaders;
    }

    public void setTimelyPrincipal(TimelyPrincipal timelyPrincipal) {
        this.timelyPrincipal = timelyPrincipal;
    }

    public TimelyPrincipal getTimelyPrincipal() {
        return timelyPrincipal;
    }

    public X509Certificate getClientCert() {
        return clientCert;
    }
}
