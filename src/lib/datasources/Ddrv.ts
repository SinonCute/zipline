import { Datasource } from '.';
import { ConfigDdrv } from 'lib/config/Config';
import Logger from 'lib/logger';
import { Readable } from 'stream';
export class Ddrv extends Datasource {
  public name = 'Ddrv';
  public logger: Logger = Logger.get('datasource::ddrv');

  public constructor(public config: ConfigDdrv) {
    super();
  }

  public async save(file: string, data: Buffer): Promise<void> {
    const fileBlob = new Blob([data], { type: 'application/octet-stream' });

    const formData = new FormData();
    formData.append('file', fileBlob, file);

    this.logger.info(`Uploading ${file} to ${this.config.bucket}`);

    const r = await fetch(`${this.config.url}/api/directories/${this.config.bucket}/files`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${this.config.key}`,
      },
      body: formData,
    });

    const j = await r.json();
    if (j.error) this.logger.error(`${j.error}: ${j.message}`);
  }

  public async delete(file: string): Promise<void> {
    const fileId: string = await this.getIdFromName(file);
    await fetch(`${this.config.url}/api/directories/${this.config.bucket}/files/${fileId}`, {
      method: 'DELETE',
      headers: {
        Authorization: `Bearer ${this.config.key}`,
      },
    });
  }

  public async clear(): Promise<void> {
    try {
      const resp = await fetch(`${this.config.url}/api/directories/${this.config.bucket}`, {
        method: 'DELETE',
        headers: {
          Authorization: `Bearer ${this.config.key}`,
          'Content-Type': 'application/json',
        },
      });
      const objs = await resp.json();
      if (objs.error) throw new Error(`${objs.error}: ${objs.message}`);

      const res = await fetch(`${this.config.url}/api/directories/`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${this.config.key}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          name: 'zipline',
          parent: `${this.config.parrent_bucket}`,
        }),
      });

      const j = await res.json();
      if (j.error) throw new Error(`${j.error}: ${j.message}`);

      return;
    } catch (e) {
      this.logger.error(e);
    }
  }

  public async get(file: string): Promise<Readable> {
    // get a readable stream from the request

    this.logger.info(`Downloading ${file} from ${this.config.bucket}`);
    const fileId: string = await this.getIdFromName(file);
    const r = await fetch(`${this.config.url}/files/${fileId}`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${this.config.key}`,
      },
    });

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return Readable.fromWeb(r.body as any);
  }

  public async size(file: string): Promise<number> {
    const fileId: string = await this.getIdFromName(file);
    return new Promise(async (res) => {
      fetch(`${this.config.url}/api/directories/${this.config.bucket}/files/${fileId}`, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${this.config.key}`,
          'Content-Type': 'application/json',
        },
      })
        .then((r) => r.json())
        .then((j) => {
          if (j.error) {
            this.logger.error(`${j.error}: ${j.message}`);
            res(0);
          }

          if (j.length === 0) {
            res(0);
          } else {
            res(j.data.size);
          }
        });
    });
  }

  public async fullSize(): Promise<number> {
    return new Promise((res) => {
      fetch(`${this.config.url}/api/directories/${this.config.bucket}`, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${this.config.key}`,
          'Content-Type': 'application/json',
        },
      })
        .then((r) => r.json())
        .then((j) => {
          if (j.error) {
            this.logger.error(`${j.error}: ${j.message}`);
            res(0);
          }

          if (j.data.files.length === 0) {
            res(0);
          }

          res(
            j.data.files.reduce((totalSize: number, file: { size: number; dir: boolean }) => {
              if (!file.dir) {
                return totalSize + file.size;
              }
              return totalSize;
            }, 0)
          );
        });
    });
  }

  public async getIdFromName(name: string): Promise<string> {
    this.logger.info(`Getting id from name: ${name}`);
    return new Promise((res) => {
      fetch(`${this.config.url}/api/directories/${this.config.bucket}`, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${this.config.key}`,
          'Content-Type': 'application/json',
        },
      })
        .then((r) => r.json())
        .then((j) => {
          if (j.error) {
            this.logger.error(`${j.error}: ${j.message}`);
            this.logger.info('No id found');
            res('');
          }

          if (j.data.files.length === 0) {
            this.logger.info('No files found');
            res('');
          }

          const id = j.data.files.find((file: { name: string; id: string; dir: boolean }) => {
            if (!file.dir) {
              this.logger.info(`Checking ${file.name}`);
              return file.name === name;
            }
            this.logger.info(`Skipping ${file.name}`);
            return false;
          });

          if (id) {
            this.logger.info(`Found id: ${id.id}`);
            res(id.id);
          } else {
            this.logger.info('No id found');
            res('');
          }
        });
    });
  }
}
