package nocomment.master;

import nocomment.master.db.Database;
import nocomment.master.network.NoCommentServer;
import nocomment.master.util.LoggingExecutor;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.util.Scanner;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class NoComment {

    public static Executor executor = new LoggingExecutor(Executors.newFixedThreadPool(48));
    public static final boolean DRY_RUN = true;

    public static void main(String[] args) throws Exception {
        if (!DRY_RUN) {
            new Database();
            NoCommentServer.listen();
        }
        makeHeatmap(15700);
    }

    public static void makeHeatmap(int radius) throws Exception {
        Scanner scan = new Scanner(new File("/Users/leijurv/Downloads/heatmap_nether_full.csv"));
        // from -radius to +radius-1
        BufferedImage output = new BufferedImage(radius * 2, radius * 2, BufferedImage.TYPE_BYTE_GRAY);
        WritableRaster raster = output.getRaster();
        SampleModel model = raster.getSampleModel();
        DataBuffer buffer = raster.getDataBuffer();
        for (int x = 0; x < 2 * radius; x++) {
            for (int y = 0; y < 2 * radius; y++) {
                model.setSample(x, y, 0, 255, buffer);
            }
        }
        int i = 0;
        while (scan.hasNextLine()) {
            if (++i % 1000000 == 0) {
                System.out.println(i);
            }
            String line = scan.nextLine();
            String[] split = line.split(",");
            int chunkX = Integer.parseInt(split[0]);
            int chunkZ = Integer.parseInt(split[1]);
            int imageX = chunkX + radius;
            int imageY = chunkZ + radius;
            if (imageX < 0 || imageX >= 2 * radius || imageY < 0 || imageY >= 2 * radius) {
                continue;
            }
            int weight = Integer.parseInt(split[2]);

            int color = 255 - (int) (50 * Math.log(weight)) - 50;

            if (color < 0) {
                color = 0;
            }
            if (color > 255) {
                color = 255;
            }
            model.setSample(imageX, imageY, 0, color, buffer);
        }
        File o = new File("/Users/leijurv/Downloads/heatmap_nether_full.png");
        ImageIO.write(output, "png", o);
    }
}
