import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Country;

import java.io.File;
import java.net.InetAddress;

public class Test {
    public static void main(String[] args) throws Exception {
        File database = new File("E:\\workspaces_new\\flinkDemo-master\\src\\main\\resources\\GeoLite2-City.mmdb");

// This reader object should be reused across lookups as creation of it is
// expensive.
        DatabaseReader reader = new DatabaseReader.Builder(database).build();

// If you want to use caching at the cost of a small (~2MB) memory overhead:
// new DatabaseReader.Builder(file).withCache(new CHMCache()).build();

        InetAddress ipAddress = InetAddress.getByName("120.244.15.12");

        CityResponse response = reader.city(ipAddress);

        Country country = response.getCountry();
        System.out.println(country.getIsoCode());
        System.out.println(response.getCity().getName());
    }
}
