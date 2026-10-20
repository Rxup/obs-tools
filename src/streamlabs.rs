// Streamlabs Desktop speaks its own JSON-RPC on port 59650, with a token. The rest of the plugin
// keeps asking for obs-websocket requests. This translates the ones a key actually needs.
use serde_json::{Value, json};

use crate::obs::{ObsClient, Reply};
use crate::{Error, Res};

#[derive(Clone, Debug)]
struct Scene {
	name: String,
	id: String,
}

#[derive(Clone, Debug)]
struct Item {
	scene: String,
	name: String,
	id: String,
	source_id: String,
	resource: String,
	visible: bool,
	horizontal: bool,
}

#[derive(Clone, Debug)]
struct Audio {
	name: String,
	source_id: String,
	resource: String,
	muted: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Cache {
	scenes: Vec<Scene>,
	items: Vec<Item>,
	audio: Vec<Audio>,
	active_id: String,
	streaming: String,
	recording: String,
}

impl Cache {
	pub fn clear(&mut self) {
		*self = Cache::default();
	}

	fn scene_name(&self, id: &str) -> Option<String> {
		self.scenes.iter().find(|scene| scene.id == id).map(|scene| scene.name.clone())
	}

	fn scene_id(&self, name: &str) -> Option<String> {
		self.scenes.iter().find(|scene| scene.name == name).map(|scene| scene.id.clone())
	}
}

pub fn call(client: &ObsClient, request_type: &str, data: Value) -> Reply {
	let Some(me) = client.rc() else {
		return Box::pin(async { Err(Error::from("OBS not connected")) });
	};
	let request_type = request_type.to_string();
	Box::pin(async move { handle(&me, &request_type, &data).await })
}

pub fn batch(client: &ObsClient, requests: Vec<Value>) -> Reply<Vec<Value>> {
	let Some(me) = client.rc() else {
		return Box::pin(async { Err(Error::from("OBS not connected")) });
	};
	Box::pin(async move {
		let mut out = Vec::new();
		for request in requests {
			let kind = request["requestType"].as_str().unwrap_or("").to_string();
			let data = request.get("requestData").cloned().unwrap_or(json!({}));
			match handle(&me, &kind, &data).await {
				Ok(response) => out.push(json!({
					"requestType": kind,
					"requestStatus": { "result": true, "code": 100 },
					"responseData": response
				})),
				Err(error) => out.push(json!({
					"requestType": kind,
					"requestStatus": { "result": false, "code": 0, "comment": error.message() },
					"responseData": Value::Null
				})),
			}
		}
		Ok(out)
	})
}

/// A pushed event, with no id. `result._type` is EVENT.
pub fn on_event(client: &ObsClient, message: &Value) {
	let event = message.get("result").filter(|value| value["_type"] == "EVENT").or_else(|| message.get("params"));
	let Some(event) = event else { return };
	let resource = event["resourceId"].as_str().unwrap_or("");
	let data = &event["data"];
	if resource.ends_with("sceneSwitched") {
		let id = data.as_str().or_else(|| data["id"].as_str()).unwrap_or("").to_string();
		let name = {
			let mut cache = client.slobs.borrow_mut();
			if !id.is_empty() {
				cache.active_id = id.clone();
			}
			cache.scene_name(&id)
		};
		if let Some(name) = name {
			client.emit("CurrentProgramSceneChanged", json!({ "sceneName": name }));
		} else {
			client.emit("SceneListChanged", json!({}));
		}
	} else if resource.ends_with("streamingStatusChange") {
		let status = text_field(data, "streamingStatus");
		client.slobs.borrow_mut().streaming = status.clone();
		client.emit("StreamStateChanged", json!({ "outputActive": output_active(&status) }));
	} else if resource.ends_with("recordingStatusChange") {
		let status = text_field(data, "recordingStatus");
		client.slobs.borrow_mut().recording = status.clone();
		client.emit("RecordStateChanged", json!({ "outputActive": output_active(&status) }));
	} else if resource.contains("SceneItem") && resource.ends_with(".visible") {
		let visible = data.as_bool().unwrap_or(false);
		let found = client.slobs.borrow().items.iter().find(|item| resource.starts_with(&item.resource)).cloned();
		if let Some(mut item) = found {
			item.visible = visible;
			let scene = item.scene.clone();
			let id = item.id.clone();
			{
				let mut cache = client.slobs.borrow_mut();
				if let Some(stored) = cache.items.iter_mut().find(|stored| stored.id == id) {
					stored.visible = visible;
				}
			}
			client.emit("SceneItemEnableStateChanged", json!({ "sceneName": scene, "sceneItemId": id, "sceneItemEnabled": visible }));
		}
	} else if resource.contains("AudioSource") && resource.ends_with(".muted") {
		let muted = data.as_bool().unwrap_or(false);
		let name = {
			let mut cache = client.slobs.borrow_mut();
			let found = cache.audio.iter_mut().find(|audio| resource.starts_with(&audio.resource));
			if let Some(audio) = found {
				audio.muted = muted;
				Some(audio.name.clone())
			} else {
				None
			}
		};
		if let Some(name) = name {
			client.emit("InputMuteStateChanged", json!({ "inputName": name, "inputMuted": muted }));
		}
	}
}

pub fn clean_error(error: &Value) -> String {
	let message = error["message"].as_str().unwrap_or("Streamlabs did not accept the command");
	message.trim_start_matches("INTERNAL_JSON_RPC_ERROR ").to_string()
}

async fn handle(client: &ObsClient, request: &str, data: &Value) -> Res<Value> {
	match request {
		"GetVersion" => Ok(json!({ "obsVersion": "Streamlabs", "platformDescription": "Streamlabs Desktop" })),
		"GetStudioModeEnabled" => Ok(json!({ "studioModeEnabled": false })),
		"GetVideoSettings" => Ok(json!({ "fpsNumerator": 30, "fpsDenominator": 1 })),
		"GetProfileList" => Ok(json!({ "currentProfileName": Value::Null, "profiles": [] })),
		"GetSceneCollectionList" => Ok(json!({ "currentSceneCollectionName": Value::Null, "sceneCollections": [] })),
		"GetStats" => Ok(json!({})),
		"GetSceneList" => {
			refresh_scenes(client).await?;
			Ok(scene_list(&client.slobs.borrow()))
		}
		"GetCurrentProgramScene" => {
			ensure_scenes(client).await?;
			let cache = client.slobs.borrow();
			let name = cache.scene_name(&cache.active_id).unwrap_or_default();
			Ok(json!({ "sceneName": name }))
		}
		"SetCurrentProgramScene" => {
			ensure_scenes(client).await?;
			let name = data["sceneName"].as_str().unwrap_or("");
			let id = client.slobs.borrow().scene_id(name).ok_or("No scene with that name in Streamlabs")?;
			client.rpc("makeSceneActive", "ScenesService", json!([id])).await?;
			client.slobs.borrow_mut().active_id = id;
			client.emit("CurrentProgramSceneChanged", json!({ "sceneName": name }));
			Ok(json!({}))
		}
		"GetSceneItemList" => {
			ensure_scenes(client).await?;
			Ok(json!({ "sceneItems": items_in(&client.slobs.borrow(), data["sceneName"].as_str().unwrap_or("")) }))
		}
		"GetSceneItemId" => {
			ensure_scenes(client).await?;
			let item =
				preferred_item(&client.slobs.borrow(), data["sceneName"].as_str().unwrap_or(""), data["sourceName"].as_str().unwrap_or(""))
					.ok_or("No source with that name in Streamlabs")?;
			Ok(json!({ "sceneItemId": item.id }))
		}
		"GetSceneItemEnabled" => {
			ensure_scenes(client).await?;
			let id = data["sceneItemId"].as_str().unwrap_or("");
			let visible = client.slobs.borrow().items.iter().find(|item| item.id == id).is_some_and(|item| item.visible);
			Ok(json!({ "sceneItemEnabled": visible }))
		}
		"SetSceneItemEnabled" => {
			ensure_scenes(client).await?;
			let id = data["sceneItemId"].as_str().unwrap_or("").to_string();
			let enabled = data["sceneItemEnabled"] == true;
			let targets: Vec<Item> = {
				let cache = client.slobs.borrow();
				let Some(item) = cache.items.iter().find(|item| item.id == id).cloned() else {
					return Err("No source with that name in Streamlabs".into());
				};
				cache.items.iter().filter(|other| other.scene == item.scene && other.name == item.name).cloned().collect()
			};
			for item in &targets {
				client.rpc("setVisibility", &item.resource, json!([enabled])).await?;
			}
			{
				let mut cache = client.slobs.borrow_mut();
				for item in &targets {
					if let Some(stored) = cache.items.iter_mut().find(|stored| stored.id == item.id) {
						stored.visible = enabled;
					}
				}
			}
			for item in &targets {
				client.emit(
					"SceneItemEnableStateChanged",
					json!({ "sceneName": item.scene, "sceneItemId": item.id, "sceneItemEnabled": enabled }),
				);
			}
			Ok(json!({}))
		}
		"GetStreamStatus" => {
			refresh_output(client).await?;
			Ok(output_status(&client.slobs.borrow().streaming))
		}
		"GetRecordStatus" => {
			refresh_output(client).await?;
			Ok(output_status(&client.slobs.borrow().recording))
		}
		"StartStream" => output_command(client, "startStreaming", true).await,
		"StopStream" => output_command(client, "stopStreaming", true).await,
		"ToggleStream" => output_command(client, "toggleStreaming", true).await,
		"StartRecord" => output_command(client, "startRecording", false).await,
		"StopRecord" => output_command(client, "stopRecording", false).await,
		"ToggleRecord" => output_command(client, "toggleRecording", false).await,
		"GetInputList" => {
			refresh_audio(client).await?;
			ensure_scenes(client).await?;
			Ok(json!({ "inputs": input_list(&client.slobs.borrow()) }))
		}
		"GetInputMute" => {
			refresh_audio(client).await?;
			let audio = find_audio(&client.slobs.borrow(), data["inputName"].as_str().unwrap_or(""))?;
			Ok(json!({ "inputMuted": audio.muted }))
		}
		"SetInputMute" => set_muted(client, data["inputName"].as_str().unwrap_or(""), data["inputMuted"] == true).await,
		"ToggleInputMute" => {
			refresh_audio(client).await?;
			let (name, muted) = {
				let audio = find_audio(&client.slobs.borrow(), data["inputName"].as_str().unwrap_or(""))?;
				(audio.name.clone(), !audio.muted)
			};
			set_muted(client, &name, muted).await?;
			Ok(json!({ "inputMuted": muted }))
		}
		_ => Err("Streamlabs does not support this".into()),
	}
}

async fn output_command(client: &ObsClient, method: &str, stream: bool) -> Res<Value> {
	client.rpc(method, "StreamingService", json!([])).await?;
	refresh_output(client).await?;
	let status = {
		let cache = client.slobs.borrow();
		if stream { cache.streaming.clone() } else { cache.recording.clone() }
	};
	let kind = if stream { "StreamStateChanged" } else { "RecordStateChanged" };
	client.emit(kind, json!({ "outputActive": output_active(&status) }));
	Ok(json!({}))
}

async fn set_muted(client: &ObsClient, name: &str, muted: bool) -> Res<Value> {
	refresh_audio(client).await?;
	let resource = find_audio(&client.slobs.borrow(), name)?.resource.clone();
	let name = name.to_string();
	client.rpc("setMuted", &resource, json!([muted])).await?;
	if let Some(audio) = client.slobs.borrow_mut().audio.iter_mut().find(|audio| audio.name == name) {
		audio.muted = muted;
	}
	client.emit("InputMuteStateChanged", json!({ "inputName": name, "inputMuted": muted }));
	Ok(json!({ "inputMuted": muted }))
}

async fn ensure_scenes(client: &ObsClient) -> Res<()> {
	if client.slobs.borrow().scenes.is_empty() {
		refresh_scenes(client).await?;
	}
	Ok(())
}

async fn refresh_scenes(client: &ObsClient) -> Res<()> {
	let scenes = client.rpc("getScenes", "ScenesService", json!([])).await?;
	let model = client.rpc("getModel", "ScenesService", json!([])).await?;
	let active = model["activeSceneId"].as_str().unwrap_or("").to_string();
	let previous = client.slobs.borrow().clone();
	*client.slobs.borrow_mut() = cache_from_scenes(&scenes, &active, previous);
	Ok(())
}

async fn refresh_audio(client: &ObsClient) -> Res<()> {
	let sources = client.rpc("getSources", "AudioService", json!([])).await?;
	let audio = match sources {
		Value::Array(items) => items.iter().filter_map(read_audio).collect(),
		_ => Vec::new(),
	};
	client.slobs.borrow_mut().audio = audio;
	Ok(())
}

async fn refresh_output(client: &ObsClient) -> Res<()> {
	let model = client.rpc("getModel", "StreamingService", json!([])).await?;
	let mut cache = client.slobs.borrow_mut();
	cache.streaming = model["streamingStatus"].as_str().unwrap_or("offline").to_string();
	cache.recording = model["recordingStatus"].as_str().unwrap_or("offline").to_string();
	Ok(())
}

fn cache_from_scenes(raw: &Value, active: &str, previous: Cache) -> Cache {
	let mut scenes = Vec::new();
	let mut items = Vec::new();
	if let Value::Array(list) = raw {
		for scene in list {
			let Some(name) = scene["name"].as_str() else { continue };
			let Some(id) = scene["id"].as_str() else { continue };
			scenes.push(Scene { name: name.to_string(), id: id.to_string() });
			if let Some(Value::Array(nodes)) = scene.get("nodes") {
				for node in nodes {
					if node["sceneNodeType"].as_str() != Some("item") {
						continue;
					}
					let Some(item_name) = node["name"].as_str() else { continue };
					let Some(item_id) = node["sceneItemId"].as_str() else { continue };
					items.push(Item {
						scene: name.to_string(),
						name: item_name.to_string(),
						id: item_id.to_string(),
						source_id: node["sourceId"].as_str().unwrap_or("").to_string(),
						resource: node["resourceId"].as_str().unwrap_or("").to_string(),
						visible: node["visible"] == true,
						horizontal: node["display"].as_str() != Some("vertical"),
					});
				}
			}
		}
	}
	Cache {
		scenes,
		items,
		active_id: active.to_string(),
		audio: previous.audio,
		streaming: previous.streaming,
		recording: previous.recording,
	}
}

fn scene_list(cache: &Cache) -> Value {
	let total = cache.scenes.len() as i64;
	let scenes: Vec<Value> = cache
		.scenes
		.iter()
		.enumerate()
		.map(|(index, scene)| json!({ "sceneName": scene.name, "sceneIndex": total - index as i64 }))
		.collect();
	let current = cache.scene_name(&cache.active_id).unwrap_or_default();
	json!({ "scenes": scenes, "currentProgramSceneName": current })
}

fn items_in(cache: &Cache, scene: &str) -> Vec<Value> {
	let mut seen = Vec::new();
	let mut out = Vec::new();
	for item in cache.items.iter().filter(|item| item.scene == scene) {
		if seen.contains(&item.name) {
			continue;
		}
		let chosen = preferred_item(cache, scene, &item.name).unwrap_or_else(|| item.clone());
		seen.push(chosen.name.clone());
		out.push(json!({
			"sceneItemId": chosen.id,
			"sourceName": chosen.name,
			"sceneItemEnabled": chosen.visible
		}));
	}
	out
}

fn preferred_item(cache: &Cache, scene: &str, name: &str) -> Option<Item> {
	let mut found: Vec<&Item> = cache.items.iter().filter(|item| item.scene == scene && item.name == name).collect();
	found.sort_by_key(|item| !item.horizontal);
	found.first().cloned().cloned()
}

fn find_audio(cache: &Cache, name: &str) -> Res<Audio> {
	cache.audio.iter().find(|audio| audio.name == name).cloned().ok_or_else(|| "No audio source with that name in Streamlabs".into())
}

fn input_list(cache: &Cache) -> Vec<Value> {
	let mut names = Vec::new();
	let mut out = Vec::new();
	for audio in &cache.audio {
		names.push(audio.name.clone());
		let kind = input_kind(&audio.source_id);
		out.push(json!({ "inputName": audio.name, "inputKind": kind, "unversionedInputKind": kind }));
	}
	for item in &cache.items {
		if names.contains(&item.name) {
			continue;
		}
		names.push(item.name.clone());
		let kind = input_kind(&item.source_id);
		out.push(json!({ "inputName": item.name, "inputKind": kind, "unversionedInputKind": kind }));
	}
	out
}

fn read_audio(value: &Value) -> Option<Audio> {
	let name = value["name"].as_str()?.to_string();
	let source_id = value["sourceId"].as_str().unwrap_or("").to_string();
	let resource = value["resourceId"].as_str().unwrap_or("").to_string();
	if resource.is_empty() {
		return None;
	}
	Some(Audio { name, source_id, resource, muted: value["muted"] == true })
}

fn input_kind(source_id: &str) -> String {
	const KINDS: &[&str] = &[
		"wasapi_process_output_capture",
		"wasapi_output_capture",
		"wasapi_input_capture",
		"coreaudio_output_capture",
		"coreaudio_input_capture",
		"browser_source",
		"ffmpeg_source",
		"game_capture",
		"dshow_input",
		"text_gdiplus",
		"image_source",
		"vlc_source",
	];
	KINDS.iter().find(|kind| source_id.starts_with(*kind)).copied().unwrap_or("streamlabs").to_string()
}

fn output_active(status: &str) -> bool {
	matches!(status, "live" | "reconnecting" | "starting" | "paused")
}

fn output_status(status: &str) -> Value {
	json!({ "outputActive": output_active(status), "outputPaused": status == "paused", "outputTimecode": "00:00:00" })
}

fn text_field(data: &Value, key: &str) -> String {
	data.as_str().or_else(|| data[key].as_str()).unwrap_or("offline").to_string()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::obs_state::sorted_scene_names;

	#[test]
	fn scene_list_keeps_streamlabs_order_and_the_live_scene() {
		let raw = json!([
			{ "id": "a", "name": "Live", "nodes": [
				{ "sceneNodeType": "item", "name": "Cam", "sceneItemId": "h", "sourceId": "dshow_input_x", "resourceId": "SceneItem[\"a\",\"h\",\"s\"]", "visible": true, "display": "horizontal" },
				{ "sceneNodeType": "item", "name": "Cam", "sceneItemId": "v", "sourceId": "dshow_input_x", "resourceId": "SceneItem[\"a\",\"v\",\"s\"]", "visible": false, "display": "vertical" }
			]},
			{ "id": "b", "name": "Pause", "nodes": [] }
		]);
		let cache = cache_from_scenes(&raw, "b", Cache::default());
		let list = scene_list(&cache);
		assert_eq!(list["currentProgramSceneName"], "Pause");
		assert_eq!(sorted_scene_names(&list), vec!["Live".to_string(), "Pause".to_string()]);
		let items = items_in(&cache, "Live");
		assert_eq!(items.len(), 1);
		assert_eq!(items[0]["sceneItemId"], "h");
		assert_eq!(items[0]["sceneItemEnabled"], true);
	}

	#[test]
	fn error_text_drops_the_internal_prefix() {
		assert_eq!(clean_error(&json!({ "message": "INTERNAL_JSON_RPC_ERROR Invalid token" })), "Invalid token");
	}
}
