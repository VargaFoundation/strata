//! A basic, **offline**, deterministic image embedder — a normalized per-channel RGB intensity
//! histogram. Not CLIP-quality, but a *real* multimodal embedding with no ONNX and no network, and a
//! drop-in [`ImageEmbeddingProvider`]. Swap in a CLIP/SigLIP encoder for semantic image search; this
//! makes the multimodal path work out of the box for near-duplicate / color-similarity retrieval.

use super::ImageEmbeddingProvider;

const BUCKETS: usize = 16; // per channel
const DIM: usize = BUCKETS * 3;

/// RGB-histogram image embedder (dimension 48).
pub struct HistogramImageEmbedding;

#[async_trait::async_trait]
impl ImageEmbeddingProvider for HistogramImageEmbedding {
    async fn embed_image(&self, bytes: &[u8]) -> crate::Result<Vec<f32>> {
        let bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || {
            let img = image::load_from_memory(&bytes)
                .map_err(|e| crate::Error::Embedding(format!("decode image: {e}")))?
                .to_rgb8();
            let mut hist = vec![0f32; DIM];
            for p in img.pixels() {
                for (c, &v) in p.0.iter().enumerate() {
                    let bucket = ((v as usize * BUCKETS) / 256).min(BUCKETS - 1);
                    hist[c * BUCKETS + bucket] += 1.0;
                }
            }
            // L2-normalize so cosine similarity is meaningful regardless of image size.
            let norm = hist.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                for x in &mut hist {
                    *x /= norm;
                }
            }
            Ok(hist)
        })
        .await
        .map_err(|e| crate::Error::Embedding(e.to_string()))?
    }

    fn dimension(&self) -> usize {
        DIM
    }

    fn model_name(&self) -> &str {
        "histogram-rgb-16"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn png(color: [u8; 3]) -> Vec<u8> {
        let img = image::RgbImage::from_pixel(8, 8, image::Rgb(color));
        let mut buf = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgb8(img)
            .write_to(&mut buf, image::ImageFormat::Png)
            .unwrap();
        buf.into_inner()
    }

    #[tokio::test]
    async fn histogram_embeds_and_distinguishes_colors() {
        let p = HistogramImageEmbedding;
        let red = p.embed_image(&png([255, 0, 0])).await.unwrap();
        let blue = p.embed_image(&png([0, 0, 255])).await.unwrap();
        assert_eq!(red.len(), 48);
        assert_eq!(p.dimension(), 48);
        // Different-colored images produce different vectors.
        assert_ne!(red, blue);
        // Same image is deterministic.
        assert_eq!(red, p.embed_image(&png([255, 0, 0])).await.unwrap());
    }
}
