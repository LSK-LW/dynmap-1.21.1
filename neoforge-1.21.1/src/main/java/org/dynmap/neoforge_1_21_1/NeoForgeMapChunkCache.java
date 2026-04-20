package org.dynmap.neoforge_1_21_1;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.level.ServerChunkCache;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.biome.Biome;
import net.minecraft.world.level.biome.BiomeSpecialEffects;
import net.minecraft.world.level.chunk.ChunkAccess;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.status.ChunkStatus;
import net.minecraft.world.level.chunk.storage.ChunkSerializer;

import org.dynmap.DynmapChunk;
import org.dynmap.Log;
import org.dynmap.common.BiomeMap;
import org.dynmap.common.chunk.GenericChunk;
import org.dynmap.common.chunk.GenericChunkCache;
import org.dynmap.common.chunk.GenericMapChunkCache;

public class NeoForgeMapChunkCache extends GenericMapChunkCache {
	private ServerLevel w;
	private ServerChunkCache cps;
	private NeoForgeWorld dw;

	public NeoForgeMapChunkCache(GenericChunkCache cc) {
		super(cc);
	}

	// 已加载区块：把 ChunkSerializer.write() 提交到主线程执行，避免 C2ME 锁竞争
	@Override
	protected GenericChunk getLoadedChunk(DynmapChunk chunk) {
		// 在主线程执行序列化，拿到 NBT 后再在渲染线程解析
		CompletableFuture<CompoundTag> future = new CompletableFuture<>();

		w.getServer().execute(() -> {
			try {
				// getChunk(..., false) 不触发加载，只读已有的
				ChunkAccess ch = cps.getChunk(chunk.x, chunk.z, ChunkStatus.FULL, false);
				if (ch instanceof LevelChunk lc) {
					// 直接从 LevelChunk 提取 NBT，绕过 C2ME 的 AsyncSerializationManager
					CompoundTag nbt = ChunkSerializer.write(w, lc);
					future.complete(nbt);
				} else {
					future.complete(null);
				}
			} catch (Exception e) {
				future.completeExceptionally(e);
			}
		});

		try {
			CompoundTag nbt = future.get(5, TimeUnit.SECONDS);
			if (nbt != null) {
				return parseChunkFromNBT(new NBT.NBTCompound(nbt));
			}
		} catch (TimeoutException e) {
			Log.severe(String.format("Timeout getting loaded chunk %d,%d", chunk.x, chunk.z));
		} catch (InterruptedException | ExecutionException e) {
			Log.severe(String.format("Error getting loaded chunk %d,%d", chunk.x, chunk.z), e);
		}
		return null;
	}

	// 未加载区块：从磁盘读，避免主线程 .join() 阻塞
	@Override
	protected GenericChunk loadChunk(DynmapChunk chunk) {
		CompoundTag nbt = readChunkAsync(chunk.x, chunk.z);
		if (nbt != null) {
			return parseChunkFromNBT(new NBT.NBTCompound(nbt));
		}
		return null;
	}

	public void setChunks(NeoForgeWorld dw, List<DynmapChunk> chunks) {
		this.dw = dw;
		this.w = dw.getWorld();
		if (dw.isLoaded()) {
			cps = this.w.getChunkSource();
		}
		super.setChunks(dw, chunks);
	}

	// 用 C2ME 的异步 read，但不在主线程 join，在 Dynmap 渲染线程等待
	private CompoundTag readChunkAsync(int x, int z) {
		try {
			// chunkMap.read() 返回 CompletableFuture，在 Dynmap 渲染线程（非主线程）等待即可
			CompoundTag rslt = cps.chunkMap.read(new ChunkPos(x, z))
					.get(10, TimeUnit.SECONDS)  // 在渲染线程等，不影响主线程
					.orElse(null);

			if (rslt != null) {
				CompoundTag lev = rslt;
				if (lev.contains("Level")) {
					lev = lev.getCompound("Level");
				}
				String stat = lev.getString("Status");
				ChunkStatus cs = ChunkStatus.byName(stat);
				if (stat == null || !cs.isOrAfter(ChunkStatus.LIGHT)) {
					return null;
				}
			}
			return rslt;
		} catch (NoSuchElementException e) {
			return null;
		} catch (TimeoutException e) {
			Log.severe(String.format("Timeout reading chunk  %s,%d,%d", dw.getName(),x, z));
			return null;
		} catch (Exception e) {
			//Log.severe(String.format("Error reading chunk %d,%d", x, z), e);
			Log.severe(String.format("Error reading chunk: %s,%d,%d", dw.getName(), x, z), e);
			return null;
		}
	}

	@Override
	public int getFoliageColor(BiomeMap bm, int[] colormap, int x, int z) {
		return bm.<Biome>getBiomeObject().map(Biome::getSpecialEffects)
				.flatMap(BiomeSpecialEffects::getFoliageColorOverride)
				.orElse(colormap[bm.biomeLookup()]);
	}

	@Override
	public int getGrassColor(BiomeMap bm, int[] colormap, int x, int z) {
		BiomeSpecialEffects effects = bm.<Biome>getBiomeObject()
				.map(Biome::getSpecialEffects).orElse(null);
		if (effects == null) return colormap[bm.biomeLookup()];
		return effects.getGrassColorModifier().modifyColor(x, z,
				effects.getGrassColorOverride().orElse(colormap[bm.biomeLookup()]));
	}
}