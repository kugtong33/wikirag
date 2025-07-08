import fs from 'node:fs';
import path from 'path';
import bz2 from 'unbzip2-stream';
import sax from 'sax';
import { QdrantClient } from '@qdrant/js-client-rest'
import { OpenAIEmbeddings } from '@langchain/openai';

const client = new QdrantClient({ url: 'http://localhost:6333' });

const embeddings = new OpenAIEmbeddings({
  openAIApiKey: process.env.OPENAI_API_KEY,
  model: 'text-embedding-3-small',
});

async function embedPage(page: Record<string, string>) {
  if (!page.text) return;

  const chunks = page.text.split(/\n\s*\n+/g).filter(p => p.trim() !== '');

  for (const chunk of chunks) {
    const vector = await embeddings.embedQuery(chunk);

    await client.upsert('wikipedia', {
      points: [
        {
          id: `${page.id}-${Math.random().toString(36).substring(2, 15)}`,
          payload: { title: page.title, text: chunk},
          vector,
        }
      ],
    })
  }
}

async function main() {
  const xmlStream = sax.createStream(true, { trim: true, normalize: true });

  let tag: string | null = null;
  let page: Record<string, string> | null = null;

  xmlStream.on("error", (error) => {
    console.error("Error parsing XML:", error);
  });

  xmlStream.on("opentag", (node) => {
    if (node.name === "page") {
      page = {};
    }
    tag = node.name;
  });

  xmlStream.on("text", (text) => {
    if (!page || !tag) return;

    switch(tag) {
      case 'title':
        page.title = text;
        break;
      case 'id':
        page.id = text;
        break;
      case 'text':
        page.text = text;
        break;
    }
  });

  xmlStream.on("closetag", (tagName) => {
    if (tagName === "page" && page) {
      console.log("xml close tag received", tagName, page.title, page.id);
      page = null;
    }
  });

  const readStream = fs.createReadStream(path.join(process.cwd(), 'enwiki-latest-pages-articles.xml.bz2')).pipe(bz2()).pipe(xmlStream);

  await new Promise((resolve, reject) => {
    readStream.on('end', resolve);
    readStream.on('error', reject);
  });
}

main()
  .then(() => {
    console.log("Migration completed successfully.");
  })
  .catch((error) => {
    console.error("Migration failed:", error);
  })
  .finally(() => {
    process.exit(0);
  });