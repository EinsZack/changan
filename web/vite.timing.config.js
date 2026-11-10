import {defineConfig} from 'vite';
export default defineConfig({preview:{proxy:{'/api':{target:'http://127.0.0.1:8410',changeOrigin:true}}}});
