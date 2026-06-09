package org.dynmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.dynmap.MapType.ImageEncoding;
import org.dynmap.hdmap.TexturePack;
import org.dynmap.storage.MapStorage;
import org.dynmap.storage.MapStorageTile;
import org.dynmap.utils.DynmapBufferedImage;
import org.dynmap.utils.ImageIOManager;
import org.dynmap.utils.MapChunkCache;
import org.dynmap.utils.RectangleVisibilityLimit;
import org.dynmap.utils.RoundVisibilityLimit;
import org.dynmap.utils.TileFlags;
import org.dynmap.utils.VisibilityLimit;
import org.dynmap.utils.Polygon;

import java.awt.image.BufferedImage;
import java.io.IOException;

public abstract class DynmapWorld {
    public List<MapType> maps = new ArrayList<MapType>();
    public List<MapTypeState> mapstate = new ArrayList<MapTypeState>();
    private int zoomOutWriteCount = 0;
    private int zoomOutBlankCount = 0;
    private int zoomOutReadAttemptCount = 0;
    private int zoomOutReadHitCount = 0;
    private int zoomOutMissingCount = 0;
    private int zoomOutWriteFailCount = 0;
    private static final int MANUAL_ZOOM_FILE_TIMEOUT_MS = 20000;
    private static final AtomicInteger zoomOutWorkerId = new AtomicInteger(0);
    private static final ExecutorService zoomOutIOExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("Dynmap ZoomOut IO " + zoomOutWorkerId.incrementAndGet());
            return t;
        }
    });


    public UpdateQueue updates = new UpdateQueue();
    public DynmapLocation center;
    public List<DynmapLocation> seedloc;    /* All seed location - both direct and based on visibility limits */
    private List<DynmapLocation> seedloccfg;    /* Configured full render seeds only */
    
    public List<VisibilityLimit> visibility_limits;
    public List<VisibilityLimit> hidden_limits;
    public MapChunkCache.HiddenChunkStyle hiddenchunkstyle;
    public int servertime;
    public boolean sendposition;
    public boolean sendhealth;
    public boolean showborder;
    private int extrazoomoutlevels;  /* Number of additional zoom out levels to generate */
    private boolean cancelled;
    private final String wname;
    private final int hashcode;
    private final String raw_wname;
    private String title;
    public int tileupdatedelay;
    private boolean is_enabled;
    boolean is_protected;   /* If true, user needs 'dynmap.world.<worldid>' privilege to see world */
    protected int[] brightnessTable = new int[16];  // 0-256 scaled brightness table
    
    private MapStorage storage; // Storage handler for this world's maps
    
    /* World height data */
    public int worldheight;	// really maxY+1
    public int minY;
    public int sealevel;
    
    protected void updateWorldHeights(int worldheight, int minY, int sealevel) {
    	this.worldheight = worldheight;
    	this.minY = minY;
    	this.sealevel = sealevel;
    }
    
    protected DynmapWorld(String wname, int worldheight, int sealevel) {
    	this(wname, worldheight, sealevel, 0);
    }
    protected DynmapWorld(String wname, int worldheight, int sealevel, int miny) {
        this.raw_wname = wname;
        this.wname = normalizeWorldName(wname);
        this.hashcode = this.wname.hashCode();
        this.title = wname;
        this.worldheight = worldheight;
        this.minY = miny;
        this.sealevel = sealevel;
        /* Generate default brightness table for surface world */
        for (int i = 0; i <= 15; ++i) {
            float f1 = 1.0F - (float)i / 15.0F;
            setBrightnessTableEntry(i, ((1.0F - f1) / (f1 * 3.0F + 1.0F)));
        }
    }
    protected void setBrightnessTableEntry(int level, float value) {
        if ((level < 0) || (level > 15)) return;
        this.brightnessTable[level] = (int)(256.0 * value);
        if (this.brightnessTable[level] > 256) this.brightnessTable[level] = 256;
        if (this.brightnessTable[level] < 0) this.brightnessTable[level] = 0;
    }
    /**
     * Get world's brightness table
     * @return table
     */
    public int[] getBrightnessTable() {
        return brightnessTable;
    }
    
    public void setExtraZoomOutLevels(int lvl) {
        extrazoomoutlevels = lvl;
    }
    public int getExtraZoomOutLevels() { return extrazoomoutlevels; }
    
    public void enqueueZoomOutUpdate(MapStorageTile tile) {
        MapTypeState mts = getMapState(tile.map);
        if (mts != null) {
            mts.setZoomOutInv(tile.x, tile.y, tile.zoom);
        }
    }

    public int getZoomOutInvCount() {
        int pending = 0;
        for (MapTypeState mts : mapstate) {
            pending += mts.getZoomOutInvCount();
        }
        return pending;
    }
         
    public int freshenZoomOutFiles() {
        return freshenZoomOutFiles(1, "timer");
    }

    public synchronized int freshenZoomOutFiles(int maxPasses) {
        return freshenZoomOutFiles(maxPasses, "direct");
    }

    public synchronized int freshenZoomOutFiles(int maxPasses, String source) {
        return freshenZoomOutFiles(maxPasses, source, Integer.MAX_VALUE);
    }

    public synchronized int freshenZoomOutFiles(int maxPasses, String source, int maxTiles) {
        int pending = getZoomOutInvCount();
        if (pending == 0) {
            return 0;
        }
        int totalProcessed = 0;
        int pass = 0;
        long lastLog = System.currentTimeMillis();
        int startReadAttempts = zoomOutReadAttemptCount;
        int startReadHits = zoomOutReadHitCount;
        int startMissing = zoomOutMissingCount;
        int startWrites = zoomOutWriteCount;
        int startWriteFails = zoomOutWriteFailCount;
        int startBlanks = zoomOutBlankCount;
        Log.info("Zoom-out freshen started for world '" + getName() + "': source=" + source + ", pending=" + pending);
        while ((pending > 0) && (pass < maxPasses) && (totalProcessed < maxTiles)) {
            int passProcessed = 0;
            MapTypeState.ZoomOutCoord c = new MapTypeState.ZoomOutCoord();
            pass++;
            for (MapTypeState mts : mapstate) {
                if (cancelled) {
                    Log.info("Zoom-out freshen cancelled for world '" + getName() + "': source=" + source + ", processed=" + totalProcessed + ", remaining=" + getZoomOutInvCount());
                    return totalProcessed;
                }
                MapType mt = mts.type;
                MapType.ImageVariant var[] = mt.getVariants();
                mts.startZoomOutIter(); // Start iterator
                while (mts.nextZoomOutInv(c)) {
                    if(cancelled) {
                        Log.info("Zoom-out freshen cancelled for world '" + getName() + "': source=" + source + ", processed=" + totalProcessed + ", remaining=" + getZoomOutInvCount());
                        return totalProcessed;
                    }
                    totalProcessed++;
                    passProcessed++;
                    long now = System.currentTimeMillis();
                    if ("manual".equals(source) && ((totalProcessed <= 10) || ((totalProcessed % 100) == 0))) {
                        Log.info("Zoom-out freshen tile start for world '" + getName() + "', map '" + mt.getName() + "': source=" + source + ", pass=" + pass + ", processed=" + totalProcessed + ", tile=" + c.x + "," + c.y + ", zoom=" + c.zoomlevel);
                    }
                    if ((totalProcessed % 100) == 0 || (now - lastLog) > 30000) {
                        Log.info("Zoom-out freshen progress for world '" + getName() + "', map '" + mt.getName() + "': source=" + source + ", pass=" + pass + ", processed=" + totalProcessed + ", tile=" + c.x + "," + c.y + ", zoom=" + c.zoomlevel);
                        lastLog = now;
                    }
                    for (int varIdx = 0; varIdx < var.length; varIdx++) {
                        MapStorageTile tile = storage.getTile(this, mt, c.x, c.y, c.zoomlevel, var[varIdx]);
                        if ("manual".equals(source)) {
                            processZoomFileWithTimeout(mts, tile, varIdx == 0);
                        }
                        else {
                            processZoomFile(mts, tile, varIdx == 0, false);
                        }
                    }
                    if (totalProcessed >= maxTiles) {
                        break;
                    }
                }
                if (totalProcessed >= maxTiles) {
                    break;
                }
            }
            pending = getZoomOutInvCount();
            Log.info("Zoom-out freshen pass complete for world '" + getName() + "': source=" + source + ", pass=" + pass + ", processed=" + passProcessed + ", remaining=" + pending + ", reads=" + (zoomOutReadAttemptCount - startReadAttempts) + ", readHits=" + (zoomOutReadHitCount - startReadHits) + ", missing=" + (zoomOutMissingCount - startMissing) + ", writes=" + (zoomOutWriteCount - startWrites) + ", writeFails=" + (zoomOutWriteFailCount - startWriteFails) + ", blankDeletes=" + (zoomOutBlankCount - startBlanks));
            if (passProcessed == 0) break;
        }
        Log.info("Zoom-out freshen finished for world '" + getName() + "': source=" + source + ", processed=" + totalProcessed + ", remaining=" + pending + ", reads=" + (zoomOutReadAttemptCount - startReadAttempts) + ", readHits=" + (zoomOutReadHitCount - startReadHits) + ", missing=" + (zoomOutMissingCount - startMissing) + ", writes=" + (zoomOutWriteCount - startWrites) + ", writeFails=" + (zoomOutWriteFailCount - startWriteFails) + ", blankDeletes=" + (zoomOutBlankCount - startBlanks));
        return totalProcessed;
    }
    
    public void cancelZoomOutFreshen() {
        cancelled = true;
    }
    
    public void activateZoomOutFreshen() {
        cancelled = false;
    }

    private static final int[] stepseq = { 3, 1, 2, 0 };

    private void processZoomFileWithTimeout(final MapTypeState mts, final MapStorageTile tile, final boolean firstVariant) {
        Future<?> future = zoomOutIOExecutor.submit(new Runnable() {
            @Override
            public void run() {
                processZoomFile(mts, tile, firstVariant, true);
            }
        });
        try {
            future.get(MANUAL_ZOOM_FILE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException tx) {
            future.cancel(true);
            zoomOutWriteFailCount++;
            Log.info("Zoom-out tile timed out and was skipped: uri=" + tile.getURI() + ", timeoutMs=" + MANUAL_ZOOM_FILE_TIMEOUT_MS);
        } catch (CancellationException cx) {
            zoomOutWriteFailCount++;
            Log.info("Zoom-out tile cancelled: uri=" + tile.getURI());
        } catch (InterruptedException ix) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            zoomOutWriteFailCount++;
            Log.info("Zoom-out tile interrupted: uri=" + tile.getURI());
        } catch (ExecutionException ex) {
            zoomOutWriteFailCount++;
            Log.severe("Zoom-out tile failed: " + tile.getURI(), ex.getCause());
        }
    }

    private void processZoomFile(MapTypeState mts, MapStorageTile tile, boolean firstVariant, boolean traceManual) {
        long mostRecentTimestamp = 0;
        int step = 1 << tile.zoom;
        MapStorageTile ztile = tile.getZoomOutTile();
        int width = mts.tileSize, height = mts.tileSize;
        BufferedImage zIm = null;
        DynmapBufferedImage kzIm = null;
        boolean blank = true;
        boolean missingRequiredTile = false;
        boolean deferIncompleteZoomOut = (MapManager.mapman != null) && MapManager.mapman.isFullOrRadiusRenderActive(getName());
        int[] argb = new int[width*height];
        int tx = ztile.x;
        int ty = ztile.y;
        ty = ty - step;   /* Adjust for negative step */ 

        /* create image buffer */
        kzIm = DynmapBufferedImage.allocateBufferedImage(width, height);
        zIm = kzIm.buf_img;
        for(int i = 0; i < 4; i++) {
            boolean doblit = true;
            int tx1 = tx + step * (1 & stepseq[i]);
            int ty1 = ty + step * (stepseq[i] >> 1);
            MapStorageTile tile1 = storage.getTile(this, tile.map, tx1, ty1, tile.zoom, tile.var);
            if (tile1 == null) continue;
            tile1.getReadLock();
            zoomOutReadAttemptCount++;
            if (traceManual) {
                Log.info("Zoom-out child read start: source=manual, parent=" + tile.getURI() + ", child=" + tile1.getURI());
            }
            if (firstVariant) { // We're handling this one - but only clear on first variant (so that we don't miss updates later)
                mts.clearZoomOutInv(tile1.x, tile1.y, tile1.zoom);
            }
            try {
                MapStorageTile.TileRead tr = tile1.read();
                if (traceManual) {
                    Log.info("Zoom-out child read done: source=manual, child=" + tile1.getURI() + ", hit=" + (tr != null));
                }
                if (tr != null) {
                    zoomOutReadHitCount++;
                    BufferedImage im = null;
                    try {
                        if (traceManual) {
                            Log.info("Zoom-out child decode start: source=manual, child=" + tile1.getURI());
                        }
                        im = ImageIOManager.imageIODecode(tr);
                        if (traceManual) {
                            Log.info("Zoom-out child decode done: source=manual, child=" + tile1.getURI() + ", image=" + ((im == null) ? "null" : (im.getWidth() + "x" + im.getHeight())));
                        }
                        // Only consider the timestamp when the tile exists and isn't broken
                        mostRecentTimestamp = Math.max(mostRecentTimestamp, tr.lastModified);
                    } catch (IOException iox) {
                        // Broken file - zap it
                        Log.info("Zoom-out child missing: reason=decode-error, child=" + tile1.getURI() + ", error=" + iox.getMessage());
                        tile1.delete();
                    }
                    if((im != null) && (im.getWidth() >= width) && (im.getHeight() >= height)) {
                        int iwidth = im.getWidth();
                        int iheight = im.getHeight();
                        if(iwidth > iheight) iwidth = iheight;
                        
                        if ((iwidth == width) && (iheight == height)) {
                            im.getRGB(0, 0, width, height, argb, 0, width);    /* Read data */
                            im.flush();
                            /* Do binlinear scale to width/2 x height/2 */
                            int off = 0;
                            for(int y = 0; y < height; y += 2) {
                                off = y*width;
                                for(int x = 0; x < width; x += 2, off += 2) {
                                    int p0 = argb[off];
                                    int p1 = argb[off+1];
                                    int p2 = argb[off+width];
                                    int p3 = argb[off+width+1];
                                    int alpha = ((p0 >> 24) & 0xFF) + ((p1 >> 24) & 0xFF) + ((p2 >> 24) & 0xFF) + ((p3 >> 24) & 0xFF);
                                    int red = ((p0 >> 16) & 0xFF) + ((p1 >> 16) & 0xFF) + ((p2 >> 16) & 0xFF) + ((p3 >> 16) & 0xFF);
                                    int green = ((p0 >> 8) & 0xFF) + ((p1 >> 8) & 0xFF) + ((p2 >> 8) & 0xFF) + ((p3 >> 8) & 0xFF);
                                    int blue = (p0 & 0xFF) + (p1 & 0xFF) + (p2 & 0xFF) + (p3 & 0xFF);
                                    argb[off>>1] = (((alpha>>2)&0xFF)<<24) | (((red>>2)&0xFF)<<16) | (((green>>2)&0xFF)<<8) | ((blue>>2)&0xFF);
                                }
                            }
                        }
                        else {
                            int[] buf = new int[iwidth * iwidth];
                            im.getRGB(0, 0, iwidth, iwidth, buf, 0, iwidth);
                            im.flush();
                            TexturePack.scaleTerrainPNGSubImage(iwidth, width/2, buf, argb);
                            /* blit scaled rendered tile onto zoom-out tile */
                            zIm.setRGB(((i>>1) != 0)?0:width/2, (i & 1) * height/2, width/2, height/2, argb, 0, width/2);
                            doblit = false;
                        }
                        blank = false;
                    }
                    else {
                        zoomOutMissingCount++;
                        missingRequiredTile = true;
                        Log.info("Zoom-out child missing: reason=" + ((im == null) ? "decode-null" : ("bad-size-" + im.getWidth() + "x" + im.getHeight())) + ", child=" + tile1.getURI() + ", expected-at-least=" + width + "x" + height);
                        if (tile1.map.getImageFormat().getEncoding() == ImageEncoding.JPG) {
                            Arrays.fill(argb, tile1.map.getBackgroundARGB(tile1.var));
                        }
                        else {
                            Arrays.fill(argb, 0);
                        }
                        tile1.delete();    // Delete unusable tile
                    }
                }
                else {
                    zoomOutMissingCount++;
                    missingRequiredTile = true;
                    Log.info("Zoom-out child missing: reason=read-null, child=" + tile1.getURI() + ", parent=" + tile.getURI());
                    if (tile1.map.getImageFormat().getEncoding() == ImageEncoding.JPG) {
                        Arrays.fill(argb, tile1.map.getBackgroundARGB(tile1.var));
                    }
                    else {
                        Arrays.fill(argb, 0);
                    }
                }
            } finally {
                tile1.releaseReadLock();
            }
            /* blit scaled rendered tile onto zoom-out tile */
            if(doblit) {
                zIm.setRGB(((i>>1) != 0)?0:width/2, (i & 1) * height/2, width/2, height/2, argb, 0, width);
            }
        }
        if (missingRequiredTile && deferIncompleteZoomOut) {
            enqueueZoomOutUpdate(tile);
            DynmapBufferedImage.freeBufferedImage(kzIm);
            return;
        }
        ztile.getWriteLock();
        try {
            MapManager mm = MapManager.mapman;
            if(mm == null)
                return;
            long crc = MapStorage.calculateImageHashCode(kzIm.argb_buf, 0, kzIm.argb_buf.length); /* Get hash of tile */
            if(blank) {
                if (ztile.exists()) {
                    ztile.delete();
                    zoomOutBlankCount++;
                    Log.info("Zoom-out tile deleted because it is blank: " + ztile.getURI());
                    MapManager.mapman.pushUpdate(this, new Client.Tile(ztile.getURI()));
                    enqueueZoomOutUpdate(ztile);
                }
            }
            else /* if (!ztile.matchesHashCode(crc)) */ {
                if (traceManual) {
                    Log.info("Zoom-out tile write start: source=manual, uri=" + ztile.getURI());
                }
                if (ztile.write(crc, zIm, (mostRecentTimestamp == 0)? System.currentTimeMillis() : mostRecentTimestamp)) {
                    zoomOutWriteCount++;
                    if ((zoomOutWriteCount <= 10) || ((zoomOutWriteCount % 100) == 0)) {
                        Log.info("Zoom-out tile write OK: count=" + zoomOutWriteCount + ", uri=" + ztile.getURI());
                    }
                    MapManager.mapman.pushUpdate(this, new Client.Tile(ztile.getURI()));
                    enqueueZoomOutUpdate(ztile);
                }
                else {
                    zoomOutWriteFailCount++;
                    Log.info("Zoom-out tile write skipped/failed: " + ztile.getURI());
                }
                if (traceManual) {
                    Log.info("Zoom-out tile write done: source=manual, uri=" + ztile.getURI());
                }
            }
        } finally {
            ztile.releaseWriteLock();
            DynmapBufferedImage.freeBufferedImage(kzIm);
        }
    }
    /* Get world name */
    public String getName() {
        return wname;
    }
    /* Test if world is nether */
    public abstract boolean isNether();

    /* Get world spawn location */
    public abstract DynmapLocation getSpawnLocation();
    
    public int hashCode() {
        return this.hashcode;
    }
    /* Get world time */
    public abstract long getTime();
    /* World is storming */
    public abstract boolean hasStorm();
    /* World is thundering */
    public abstract boolean isThundering();
    /* World is loaded */
    public abstract boolean isLoaded();
    /* Set world unloaded */
    public abstract void setWorldUnloaded();
    /* Get light level of block */
    public abstract int getLightLevel(int x, int y, int z);
    /* Get highest Y coord of given location */
    public abstract int getHighestBlockYAt(int x, int z);
    /* Test if sky light level is requestable */
    public abstract boolean canGetSkyLightLevel();
    /* Return sky light level */
    public abstract int getSkyLightLevel(int x, int y, int z);
    /**
     * Get world environment ID (lower case - normal, the_end, nether)
     * @return environment ID
     */
    public abstract String getEnvironment();
    /**
     * Get map chunk cache for world
     * @param chunks - list of chunks to load
     * @return cache
     */
    public abstract MapChunkCache getChunkCache(List<DynmapChunk> chunks);

    /**
     * Get title for world
     * @return title
     */
    public String getTitle() {
        return title;
    }
    /**
     * Get center location
     * @return center
     */
    public DynmapLocation getCenterLocation() {
        if(center != null)
            return center;
        else
            return getSpawnLocation();
    }
    
    /* Load world configuration from configuration node */
    public boolean loadConfiguration(DynmapCore core, ConfigurationNode worldconfig) {
        is_enabled = worldconfig.getBoolean("enabled", false); 
        if (!is_enabled) {
            return false;
        }
        title = worldconfig.getString("title", title);
        ConfigurationNode ctr = worldconfig.getNode("center");
        int mid_y = (worldheight + minY)/2;
        if(ctr != null)
            center = new DynmapLocation(wname, ctr.getDouble("x", 0.0), ctr.getDouble("y", mid_y), ctr.getDouble("z", 0));
        else
            center = null;
        List<ConfigurationNode> loclist = worldconfig.getNodes("fullrenderlocations");
        seedloc = new ArrayList<DynmapLocation>();
        seedloccfg = new ArrayList<DynmapLocation>();
        servertime = (int)(getTime() % 24000);
        sendposition = worldconfig.getBoolean("sendposition", true);
        sendhealth = worldconfig.getBoolean("sendhealth", true);
        showborder = worldconfig.getBoolean("showborder", true);
        is_protected = worldconfig.getBoolean("protected", false);
        setExtraZoomOutLevels(worldconfig.getInteger("extrazoomout", 0));
        setTileUpdateDelay(worldconfig.getInteger("tileupdatedelay", -1));
        storage = core.getDefaultMapStorage();
        if(loclist != null) {
            for(ConfigurationNode loc : loclist) {
                DynmapLocation lx = new DynmapLocation(wname, loc.getDouble("x", 0), loc.getDouble("y", mid_y), loc.getDouble("z", 0));
                seedloc.add(lx); /* Add to both combined and configured seed list */
                seedloccfg.add(lx);
            }
        }
        /* Build maps */
        maps.clear();
        Log.verboseinfo("Loading maps of world '" + wname + "'...");
        for(MapType map : worldconfig.<MapType>createInstances("maps", new Class<?>[] { DynmapCore.class }, new Object[] { core })) {
            if(map.getName() != null) {
                maps.add(map);
            }
        }        
        /* Rebuild map state list - match on indexes */
        mapstate.clear();
        for(MapType map : maps) {
            MapTypeState ms = new MapTypeState(this, map);
            ms.setInvalidatePeriod(map.getTileUpdateDelay(this));
            mapstate.add(ms);
        }
        Log.info("Loaded " + maps.size() + " maps of world '" + wname + "'.");
        /* Load visibility limits, if any are defined */
        List<ConfigurationNode> vislimits = worldconfig.getNodes("visibilitylimits");
        if(vislimits != null) {
            visibility_limits = new ArrayList<VisibilityLimit>();
            for(ConfigurationNode vis : vislimits) {
                VisibilityLimit lim;
                if (vis.containsKey("r")) {  /* It is round visibility limit */
                    int x_center = vis.getInteger("x", 0);
                    int z_center = vis.getInteger("z", 0);
                    int radius = vis.getInteger("r", 0);
                    lim = new RoundVisibilityLimit(x_center, z_center, radius);
                }
                else {  /* Rectangle visibility limit */
                    int x0 = vis.getInteger("x0", 0);
                    int x1 = vis.getInteger("x1", 0);
                    int z0 = vis.getInteger("z0", 0);
                    int z1 = vis.getInteger("z1", 0);
                    lim = new RectangleVisibilityLimit(x0, z0, x1, z1);
                }
                visibility_limits.add(lim);
                /* Also, add a seed location for the middle of each visible area */
                seedloc.add(new DynmapLocation(wname, lim.xCenter(), 64, lim.zCenter()));
            }            
        }
        /* Load hidden limits, if any are defined */
        List<ConfigurationNode> hidelimits = worldconfig.getNodes("hiddenlimits");
        if(hidelimits != null) {
            hidden_limits = new ArrayList<VisibilityLimit>();
            for(ConfigurationNode vis : hidelimits) {
                VisibilityLimit lim;
                if (vis.containsKey("r")) {  /* It is round visibility limit */
                    int x_center = vis.getInteger("x", 0);
                    int z_center = vis.getInteger("z", 0);
                    int radius = vis.getInteger("r", 0);
                    lim = new RoundVisibilityLimit(x_center, z_center, radius);
                }
                else {  /* Rectangle visibility limit */
                    int x0 = vis.getInteger("x0", 0);
                    int x1 = vis.getInteger("x1", 0);
                    int z0 = vis.getInteger("z0", 0);
                    int z1 = vis.getInteger("z1", 0);
                    lim = new RectangleVisibilityLimit(x0, z0, x1, z1);
                }
                hidden_limits.add(lim);
            }            
        }
        String hiddenchunkstyle = worldconfig.getString("hidestyle", "stone");
        this.hiddenchunkstyle = MapChunkCache.HiddenChunkStyle.fromValue(hiddenchunkstyle);
        if (this.hiddenchunkstyle == null) this.hiddenchunkstyle = MapChunkCache.HiddenChunkStyle.FILL_STONE_PLAIN;
        
        return true;
    }
    /*
     * Make configuration node for saving world
     */
    public ConfigurationNode saveConfiguration() {
        ConfigurationNode node = new ConfigurationNode();
        /* Add name and title */
        node.put("name", wname);
        node.put("title", getTitle());
        node.put("enabled", is_enabled);
        node.put("protected", is_protected);
        node.put("showborder", showborder);
        if(tileupdatedelay > 0) {
            node.put("tileupdatedelay",  tileupdatedelay);
        }
        /* Add center */
        if(center != null) {
            ConfigurationNode c = new ConfigurationNode();
            c.put("x", center.x);
            c.put("y", center.y);
            c.put("z", center.z);
            node.put("center", c.entries);
        }
        /* Add seed locations, if any */
        if(seedloccfg.size() > 0) {
            ArrayList<Map<String,Object>> locs = new ArrayList<Map<String,Object>>();
            for(int i = 0; i < seedloccfg.size(); i++) {
                DynmapLocation dl = seedloccfg.get(i);
                ConfigurationNode ll = new ConfigurationNode();
                ll.put("x", dl.x);
                ll.put("y", dl.y);
                ll.put("z", dl.z);
                locs.add(ll.entries);
            }
            node.put("fullrenderlocations", locs);
        }
        /* Add flags */
        node.put("sendposition", sendposition);
        node.put("sendhealth", sendhealth);
        node.put("extrazoomout", extrazoomoutlevels);
        /* Save visibility limits, if defined */
        if(visibility_limits != null) {
            ArrayList<Map<String,Object>> lims = new ArrayList<Map<String,Object>>();
            for(int i = 0; i < visibility_limits.size(); i++) {
                VisibilityLimit lim = visibility_limits.get(i);
                LinkedHashMap<String, Object> lv = new LinkedHashMap<String,Object>();
                if (lim instanceof RectangleVisibilityLimit) {
                    RectangleVisibilityLimit rect_lim = (RectangleVisibilityLimit) lim;
                    lv.put("x0", rect_lim.x_min);
                    lv.put("z0", rect_lim.z_min);
                    lv.put("x1", rect_lim.x_max);
                    lv.put("z1", rect_lim.z_max);
                }
                else {
                    RoundVisibilityLimit round_lim = (RoundVisibilityLimit) lim;
                    lv.put("x", round_lim.x_center);
                    lv.put("z", round_lim.z_center);
                    lv.put("r", round_lim.radius);
                }
                lims.add(lv);
            }
            node.put("visibilitylimits", lims);
        }
        /* Save hidden limits, if defined */
        if(hidden_limits != null) {
            ArrayList<Map<String,Object>> lims = new ArrayList<Map<String,Object>>();
            for(int i = 0; i < hidden_limits.size(); i++) {
                VisibilityLimit lim = hidden_limits.get(i);
                LinkedHashMap<String, Object> lv = new LinkedHashMap<String,Object>();
                if (lim instanceof RectangleVisibilityLimit) {
                    RectangleVisibilityLimit rect_lim = (RectangleVisibilityLimit) lim;
                    lv.put("x0", rect_lim.x_min);
                    lv.put("z0", rect_lim.z_min);
                    lv.put("x1", rect_lim.x_max);
                    lv.put("z1", rect_lim.z_max);
                }
                else {
                    RoundVisibilityLimit round_lim = (RoundVisibilityLimit) lim;
                    lv.put("x", round_lim.x_center);
                    lv.put("z", round_lim.z_center);
                    lv.put("r", round_lim.radius);
                }
                lims.add(lv);
            }
            node.put("hiddenlimits", lims);
        }
        /* Handle hide style */
        node.put("hidestyle", hiddenchunkstyle.getValue());
        /* Handle map settings */
        ArrayList<Map<String,Object>> mapinfo = new ArrayList<Map<String,Object>>();
        for(MapType mt : maps) {
            ConfigurationNode mnode = mt.saveConfiguration();
            mapinfo.add(mnode);
        }
        node.put("maps", mapinfo);

        return node;
    }
    public boolean isEnabled() {
        return is_enabled;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public static String normalizeWorldName(String n) {
        return (n != null)?n.replace('/', '-').replace('[', '_').replace(']', '_'):null;
    }
    public String getRawName() {
        return raw_wname;
    }
    public boolean isProtected() {
        return is_protected;
    }
    public int getTileUpdateDelay() {
        if(tileupdatedelay > 0)
            return tileupdatedelay;
        else
            return MapManager.mapman.getDefTileUpdateDelay();
    }
    public void setTileUpdateDelay(int time_sec) {
        tileupdatedelay = time_sec;
    }
    public static void doInitialScan(boolean doscan) {
    }
    // Return number of chunks found (-1 if not implemented)
    public int getChunkMap(TileFlags map) {
        return -1;
    }
    // Get map state for given map
    public MapTypeState getMapState(MapType m) {
        for (int i = 0; i < this.maps.size(); i++) {
            MapType mt = this.maps.get(i);
            if (mt == m) {
                return this.mapstate.get(i);
            }
        }
        return null;
    }
    
    public void purgeTree() {
        storage.purgeMapTiles(this, null);
    }

    public void purgeMap(MapType mt) {
        storage.purgeMapTiles(this, mt);
    }

    public MapStorage getMapStorage() {
        return storage;
    }
    
    public Polygon  getWorldBorder() {
        return null;
    }
}
