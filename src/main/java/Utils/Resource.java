package Utils;

import java.net.URL;
import java.nio.file.Paths;

class Resource {
    static String getPath(String path){
        URL resource = Resource.class.getClassLoader().getResource(path);

        if (resource==null) {
            throw new ResourceNotFoundException(path);
        }

        try {
            return Paths.get(resource.toURI()).toString();
        } catch (Exception e) {
            return resource.getPath();
        }
    }

    private static class ResourceNotFoundException extends RuntimeException {
        private ResourceNotFoundException(String message) {
            super(message);
        }
    }
}
